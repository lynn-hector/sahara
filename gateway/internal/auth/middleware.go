package auth

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

type contextKey string

const claimsKey contextKey = "sahara.claims"

// ── Context helpers ─────────────────────────────────

// ClaimsFromContext returns the JWT claims stored by the auth middleware.
// Returns nil if no claims are present (unauthenticated or API-key mode).
func ClaimsFromContext(ctx context.Context) *Claims {
	c, _ := ctx.Value(claimsKey).(*Claims)
	return c
}

func contextWithClaims(ctx context.Context, c *Claims) context.Context {
	return context.WithValue(ctx, claimsKey, c)
}

// ── Authenticator ───────────────────────────────────

// Authenticator decides how to authenticate HTTP/WS requests.
// It supports three modes (checked in order):
//  1. JWT mode:     jwtMgr != nil → validate Bearer JWT
//  2. API Key mode: apiKey != "" → match static key
//  3. Open mode:    both nil/empty → allow all (dev only)
type Authenticator struct {
	jwtMgr *JWTManager
	apiKey string
}

// NewAuthenticator creates an authenticator.
// Pass nil for jwtMgr and "" for apiKey to disable authentication.
func NewAuthenticator(jwtMgr *JWTManager, apiKey string) *Authenticator {
	return &Authenticator{jwtMgr: jwtMgr, apiKey: apiKey}
}

// Mode returns a human-readable description of the active auth mode.
func (a *Authenticator) Mode() string {
	if a.jwtMgr != nil {
		return "jwt"
	}
	if a.apiKey != "" {
		return "api_key"
	}
	return "open"
}

// Middleware returns an HTTP middleware that enforces authentication.
// Unauthenticated requests receive a 401 JSON response.
// Paths listed in skipPaths are excluded from auth checks (e.g. healthz).
func (a *Authenticator) Middleware(skipPaths map[string]bool) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if skipPaths[r.URL.Path] {
				next.ServeHTTP(w, r)
				return
			}

			claims, err := a.Authenticate(r)
			if err != nil {
				writeAuthError(w, http.StatusUnauthorized, err.Error())
				return
			}

			if claims != nil {
				r = r.WithContext(contextWithClaims(r.Context(), claims))
			}
			next.ServeHTTP(w, r)
		})
	}
}

// Authenticate validates the request credentials and returns claims (JWT mode)
// or nil (API key / open mode). Returns an error if authentication fails.
func (a *Authenticator) Authenticate(r *http.Request) (*Claims, error) {
	token := extractBearerToken(r)

	// JWT mode
	if a.jwtMgr != nil {
		if token == "" {
			return nil, errMissingToken
		}
		claims, err := a.jwtMgr.Validate(token)
		if err != nil {
			return nil, err
		}
		return claims, nil
	}

	// API Key mode
	if a.apiKey != "" {
		if token == "" {
			return nil, errMissingToken
		}
		if token != a.apiKey {
			return nil, errInvalidAPIKey
		}
		return nil, nil // authenticated but no claims
	}

	// Open mode
	return nil, nil
}

// AuthenticateWS is like Authenticate but also checks query params,
// which is common for WebSocket connections where headers are limited.
func (a *Authenticator) AuthenticateWS(r *http.Request) (*Claims, error) {
	token := extractBearerToken(r)
	if token == "" {
		token = r.URL.Query().Get("token")
	}

	if a.jwtMgr != nil {
		if token == "" {
			return nil, errMissingToken
		}
		return a.jwtMgr.Validate(token)
	}

	if a.apiKey != "" {
		if token == "" {
			return nil, errMissingToken
		}
		if token != a.apiKey {
			return nil, errInvalidAPIKey
		}
		return nil, nil
	}

	return nil, nil
}

// ── Dev token endpoint ──────────────────────────────

// DevTokenHandler returns an HTTP handler that issues JWT tokens for development.
// Only enabled when devMode is true; otherwise returns 404.
func DevTokenHandler(jwtMgr *JWTManager, devMode bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !devMode || jwtMgr == nil {
			http.NotFound(w, r)
			return
		}

		var req struct {
			UserID string   `json:"user_id"`
			Roles  []string `json:"roles"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeAuthError(w, http.StatusBadRequest, "invalid request body")
			return
		}
		if req.UserID == "" {
			req.UserID = "dev-user"
		}
		if len(req.Roles) == 0 {
			req.Roles = []string{"user"}
		}

		accessToken, err := jwtMgr.IssueAccessToken(req.UserID, req.Roles, nil)
		if err != nil {
			slog.Error("dev token issue failed", "err", err)
			writeAuthError(w, http.StatusInternalServerError, "failed to issue token")
			return
		}

		refreshToken, err := jwtMgr.IssueRefreshToken(req.UserID)
		if err != nil {
			slog.Error("dev refresh token issue failed", "err", err)
			writeAuthError(w, http.StatusInternalServerError, "failed to issue refresh token")
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"access_token":  accessToken,
			"refresh_token": refreshToken,
			"token_type":    "Bearer",
			"expires_in":    int(15 * time.Minute / time.Second),
		})
	}
}

// RefreshHandler returns an HTTP handler that exchanges a refresh token
// for a new access token + refresh token pair.
func RefreshHandler(jwtMgr *JWTManager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if jwtMgr == nil {
			http.NotFound(w, r)
			return
		}

		var req struct {
			RefreshToken string `json:"refresh_token"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeAuthError(w, http.StatusBadRequest, "invalid request body")
			return
		}
		if req.RefreshToken == "" {
			writeAuthError(w, http.StatusBadRequest, "refresh_token is required")
			return
		}

		userID, err := jwtMgr.ValidateRefreshToken(req.RefreshToken)
		if err != nil {
			writeAuthError(w, http.StatusUnauthorized, "invalid refresh token: "+err.Error())
			return
		}

		accessToken, err := jwtMgr.IssueAccessToken(userID, []string{"user"}, nil)
		if err != nil {
			writeAuthError(w, http.StatusInternalServerError, "failed to issue token")
			return
		}

		newRefresh, err := jwtMgr.IssueRefreshToken(userID)
		if err != nil {
			writeAuthError(w, http.StatusInternalServerError, "failed to issue refresh token")
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"access_token":  accessToken,
			"refresh_token": newRefresh,
			"token_type":    "Bearer",
			"expires_in":    int(15 * time.Minute / time.Second),
		})
	}
}

// ── Helpers ─────────────────────────────────────────

var (
	errMissingToken  = &authError{msg: "missing authentication token"}
	errInvalidAPIKey = &authError{msg: "invalid API key"}
)

type authError struct{ msg string }

func (e *authError) Error() string { return e.msg }

func extractBearerToken(r *http.Request) string {
	auth := r.Header.Get("Authorization")
	if strings.HasPrefix(auth, "Bearer ") {
		return strings.TrimPrefix(auth, "Bearer ")
	}
	return ""
}

func writeAuthError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]any{
		"error": map[string]any{
			"message": message,
			"type":    "authentication_error",
		},
	})
}
