package auth

import (
	"testing"
	"time"
)

func TestJWTSignValidateCycle(t *testing.T) {
	mgr, err := NewJWTManager(ManagerConfig{
		Secret: "test-secret-32-bytes-long-enough",
	})
	if err != nil {
		t.Fatal(err)
	}

	token, err := mgr.IssueAccessToken("user-123", []string{"user", "premium"}, &Quota{
		Tier:                "pro",
		MaxSubmitsPerMinute: 30,
	})
	if err != nil {
		t.Fatal(err)
	}

	claims, err := mgr.Validate(token)
	if err != nil {
		t.Fatal("validate failed:", err)
	}

	if claims.UserID != "user-123" {
		t.Errorf("got UserID=%q, want user-123", claims.UserID)
	}
	if !claims.HasRole("user") {
		t.Error("expected HasRole(user)=true")
	}
	if claims.HasRole("admin") {
		t.Error("expected HasRole(admin)=false")
	}
	if claims.Quota == nil || claims.Quota.Tier != "pro" {
		t.Error("quota not preserved")
	}
}

func TestJWTTamperedToken(t *testing.T) {
	mgr, _ := NewJWTManager(ManagerConfig{Secret: "my-secret-key-for-testing"})
	token, _ := mgr.IssueAccessToken("u1", []string{"user"}, nil)
	_, err := mgr.Validate(token + "x")
	if err == nil {
		t.Error("expected error for tampered token")
	}
}

func TestJWTRefreshToken(t *testing.T) {
	mgr, _ := NewJWTManager(ManagerConfig{Secret: "my-secret-key-for-testing"})
	rt, err := mgr.IssueRefreshToken("user-456")
	if err != nil {
		t.Fatal(err)
	}
	userID, err := mgr.ValidateRefreshToken(rt)
	if err != nil {
		t.Fatal("refresh validate failed:", err)
	}
	if userID != "user-456" {
		t.Errorf("got userID=%q, want user-456", userID)
	}
}

func TestJWTIsExpiring(t *testing.T) {
	mgr, _ := NewJWTManager(ManagerConfig{
		Secret:        "test-key",
		AccessTTL:     30 * time.Second,
		ExpiryWarning: 60 * time.Second,
	})
	token, _ := mgr.IssueAccessToken("u", []string{"user"}, nil)
	claims, _ := mgr.Validate(token)
	if !mgr.IsExpiring(claims) {
		t.Error("token with 30s TTL and 60s warning should be expiring")
	}
}

func TestJWTWrongSecret(t *testing.T) {
	mgr1, _ := NewJWTManager(ManagerConfig{Secret: "secret-1"})
	mgr2, _ := NewJWTManager(ManagerConfig{Secret: "secret-2"})
	token, _ := mgr1.IssueAccessToken("u", []string{"user"}, nil)
	_, err := mgr2.Validate(token)
	if err == nil {
		t.Error("expected error when validating with wrong secret")
	}
}

func TestNewJWTManagerMissingSecret(t *testing.T) {
	_, err := NewJWTManager(ManagerConfig{})
	if err == nil {
		t.Error("expected error for empty secret")
	}
}
