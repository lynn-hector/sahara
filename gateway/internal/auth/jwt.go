// Package auth implements JWT-based authentication for the Sahara Gateway.
//
// Auth modes (determined by config):
//   - JWT mode:    JWT_SECRET is set → tokens signed with HS256, full lifecycle
//   - API Key mode: JWT_SECRET is empty, API_KEY is set → simple bearer token match
//   - Open mode:   both empty → no authentication (local dev only)
//
// In JWT mode the Gateway can both validate tokens (normal flow, tokens signed
// by sahara-api) and issue them directly (dev mode only, for testing).
package auth

import (
	"errors"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// ── Claims ──────────────────────────────────────────

// Claims carries user identity and authorization info inside a JWT.
// sahara-api signs these; the Gateway only reads (except in dev mode).
type Claims struct {
	UserID string   `json:"sub"`
	Roles  []string `json:"roles"`
	Quota  *Quota   `json:"quota,omitempty"`
	jwt.RegisteredClaims
}

// Quota embeds per-user rate/usage limits directly in the token
// so the Gateway can enforce them without a DB lookup.
type Quota struct {
	MaxConcurrentSessions int    `json:"maxConcurrentSessions,omitempty"`
	MaxSubmitsPerMinute   int    `json:"maxSubmitsPerMinute,omitempty"`
	Tier                  string `json:"tier,omitempty"` // "free" | "pro" | "enterprise"
}

// HasRole returns true if the claims include the given role.
func (c *Claims) HasRole(role string) bool {
	for _, r := range c.Roles {
		if r == role {
			return true
		}
	}
	return false
}

// ── JWTManager ──────────────────────────────────────

// JWTManager handles signing and validation of JWTs using HS256.
type JWTManager struct {
	secret        []byte
	issuer        string
	audience      string
	accessTTL     time.Duration
	refreshTTL    time.Duration
	expiryWarning time.Duration
}

// ManagerConfig holds parameters for creating a JWTManager.
type ManagerConfig struct {
	Secret        string        // HS256 signing key (required)
	Issuer        string        // "iss" claim value
	Audience      string        // "aud" claim value
	AccessTTL     time.Duration // access token lifetime (default 15m)
	RefreshTTL    time.Duration // refresh token lifetime (default 7d)
	ExpiryWarning time.Duration // warn client this long before expiry (default 60s)
}

// NewJWTManager creates a manager with the given configuration.
func NewJWTManager(cfg ManagerConfig) (*JWTManager, error) {
	if cfg.Secret == "" {
		return nil, errors.New("JWT secret is required")
	}
	if cfg.Issuer == "" {
		cfg.Issuer = "sahara"
	}
	if cfg.Audience == "" {
		cfg.Audience = "sahara-gateway"
	}
	if cfg.AccessTTL == 0 {
		cfg.AccessTTL = 15 * time.Minute
	}
	if cfg.RefreshTTL == 0 {
		cfg.RefreshTTL = 7 * 24 * time.Hour
	}
	if cfg.ExpiryWarning == 0 {
		cfg.ExpiryWarning = 60 * time.Second
	}
	return &JWTManager{
		secret:        []byte(cfg.Secret),
		issuer:        cfg.Issuer,
		audience:      cfg.Audience,
		accessTTL:     cfg.AccessTTL,
		refreshTTL:    cfg.RefreshTTL,
		expiryWarning: cfg.ExpiryWarning,
	}, nil
}

// IssueAccessToken creates a signed access JWT for the given user.
func (m *JWTManager) IssueAccessToken(userID string, roles []string, quota *Quota) (string, error) {
	now := time.Now()
	claims := Claims{
		UserID: userID,
		Roles:  roles,
		Quota:  quota,
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    m.issuer,
			Audience:  jwt.ClaimStrings{m.audience},
			Subject:   userID,
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(m.accessTTL)),
		},
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString(m.secret)
}

// IssueRefreshToken creates a long-lived refresh token.
func (m *JWTManager) IssueRefreshToken(userID string) (string, error) {
	now := time.Now()
	claims := jwt.RegisteredClaims{
		Issuer:    m.issuer,
		Audience:  jwt.ClaimStrings{m.audience},
		Subject:   userID,
		IssuedAt:  jwt.NewNumericDate(now),
		ExpiresAt: jwt.NewNumericDate(now.Add(m.refreshTTL)),
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString(m.secret)
}

// Validate parses and validates an access token, returning its claims.
func (m *JWTManager) Validate(tokenString string) (*Claims, error) {
	token, err := jwt.ParseWithClaims(tokenString, &Claims{}, func(t *jwt.Token) (any, error) {
		if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", t.Header["alg"])
		}
		return m.secret, nil
	},
		jwt.WithIssuer(m.issuer),
		jwt.WithAudience(m.audience),
		jwt.WithExpirationRequired(),
	)
	if err != nil {
		return nil, fmt.Errorf("invalid token: %w", err)
	}

	claims, ok := token.Claims.(*Claims)
	if !ok || !token.Valid {
		return nil, errors.New("invalid token claims")
	}

	return claims, nil
}

// ValidateRefreshToken validates a refresh token and returns the subject (userID).
func (m *JWTManager) ValidateRefreshToken(tokenString string) (string, error) {
	token, err := jwt.ParseWithClaims(tokenString, &jwt.RegisteredClaims{}, func(t *jwt.Token) (any, error) {
		if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", t.Header["alg"])
		}
		return m.secret, nil
	},
		jwt.WithIssuer(m.issuer),
		jwt.WithAudience(m.audience),
		jwt.WithExpirationRequired(),
	)
	if err != nil {
		return "", fmt.Errorf("invalid refresh token: %w", err)
	}

	claims, ok := token.Claims.(*jwt.RegisteredClaims)
	if !ok || !token.Valid {
		return "", errors.New("invalid refresh token claims")
	}

	return claims.Subject, nil
}

// IsExpiring returns true if the token will expire within the warning window.
func (m *JWTManager) IsExpiring(claims *Claims) bool {
	if claims.ExpiresAt == nil {
		return false
	}
	return time.Until(claims.ExpiresAt.Time) < m.expiryWarning
}

// ExpiryWarning returns the configured warning duration.
func (m *JWTManager) ExpiryWarning() time.Duration {
	return m.expiryWarning
}
