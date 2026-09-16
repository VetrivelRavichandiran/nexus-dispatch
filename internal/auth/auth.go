// Package auth implements JWT authentication and role-based authorization.
//
// Roles: USER, DRIVER, ADMIN. Tokens are signed (HS256 in dev; the code is
// RS256-ready via the KeyFunc). The gateway verifies tokens and enforces
// per-route roles. WebSocket handshakes require a short-lived token.
package auth

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// Role is an authorization role.
type Role string

const (
	RoleUser   Role = "USER"
	RoleDriver Role = "DRIVER"
	RoleAdmin  Role = "ADMIN"
)

// Claims are the JWT claims used by NEXUS-DISPATCH.
type Claims struct {
	UserID string `json:"uid"`
	Role   Role   `json:"role"`
	jwt.RegisteredClaims
}

// Issuer is the token issuer.
const Issuer = "nexus-dispatch"

// TokenManager issues and verifies JWTs.
type TokenManager struct {
	secret   []byte
	issuer   string
	expiry   time.Duration
}

// NewTokenManager creates a manager. secret must be non-empty (env-provided).
func NewTokenManager(secret string, expiry time.Duration) *TokenManager {
	if expiry == 0 {
		expiry = 24 * time.Hour
	}
	return &TokenManager{secret: []byte(secret), issuer: Issuer, expiry: expiry}
}

// Issue creates a signed token for a user.
func (m *TokenManager) Issue(userID string, role Role) (string, error) {
	now := time.Now()
	claims := Claims{
		UserID: userID,
		Role:   role,
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    m.issuer,
			Subject:   userID,
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(m.expiry)),
		},
	}
	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	s, err := tok.SignedString(m.secret)
	if err != nil {
		return "", fmt.Errorf("auth: sign: %w", err)
	}
	return s, nil
}

// ErrInvalid is returned for any token verification failure.
var ErrInvalid = errors.New("auth: invalid token")

// Verify parses and validates a token, returning its claims.
func (m *TokenManager) Verify(token string) (*Claims, error) {
	claims := &Claims{}
	tok, err := jwt.ParseWithClaims(token, claims, func(t *jwt.Token) (any, error) {
		if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("auth: unexpected signing method %v", t.Header["alg"])
		}
		return m.secret, nil
	}, jwt.WithIssuer(m.issuer), jwt.WithValidMethods([]string{jwt.SigningMethodHS256.Alg()}))
	if err != nil || !tok.Valid {
		return nil, ErrInvalid
	}
	return claims, nil
}

// Context keys for carrying claims through a request.
type ctxKey int

const (
	ctxUserID ctxKey = iota
	ctxRole
)

// ContextWithClaims stores claims in a context.
func ContextWithClaims(ctx context.Context, c *Claims) context.Context {
	ctx = context.WithValue(ctx, ctxUserID, c.UserID)
	ctx = context.WithValue(ctx, ctxRole, c.Role)
	return ctx
}

// UserIDFromContext returns the authenticated user id ("" if absent).
func UserIDFromContext(ctx context.Context) string {
	v, _ := ctx.Value(ctxUserID).(string)
	return v
}

// RoleFromContext returns the authenticated role ("" if absent).
func RoleFromContext(ctx context.Context) Role {
	v, _ := ctx.Value(ctxRole).(Role)
	return v
}

// RequireRole checks that ctx carries one of the allowed roles.
func RequireRole(ctx context.Context, roles ...Role) error {
	r := RoleFromContext(ctx)
	for _, allowed := range roles {
		if r == allowed {
			return nil
		}
	}
	return fmt.Errorf("auth: role %q not in %v", r, roles)
}