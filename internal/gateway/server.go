package gateway

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/nexus-dispatch/nexus-dispatch/internal/auth"
	"github.com/nexus-dispatch/nexus-dispatch/internal/config"
	"github.com/nexus-dispatch/nexus-dispatch/internal/metrics"
	"github.com/nexus-dispatch/nexus-dispatch/internal/redis"
)

// Server is the API Gateway.
type Server struct {
	cfg      config.Config
	log      *slog.Logger
	tokens   *auth.TokenManager
	store    *redis.Store
	hub      *Hub
	common   *metrics.Common
	// gRPC clients
	driverClient DriverClient
	rideClient   RideClient
	locClient    LocationClient
	dispatch     DispatchClient
}

// gRPC client interfaces (implemented in grpc_clients.go).
type DriverClient interface {
	RegisterDriver(ctx context.Context, name, email, make, model, plate string, seats int, lat, lng float64) (id, state string, err error)
	UpdateDriverStatus(ctx context.Context, driverID string, state string) (prev, cur string, err error)
	GetDriver(ctx context.Context, driverID string) (map[string]any, error)
}
type RideClient interface {
	CreateRide(ctx context.Context, userID string, pickupLat, pickupLng, dropLat, dropLng float64, radius int) (id string, err error)
	GetRide(ctx context.Context, rideID string) (map[string]any, error)
	CancelRide(ctx context.Context, rideID, userID, reason string) (map[string]any, error)
}
type LocationClient interface {
	UpdateLocation(ctx context.Context, driverID string, lat, lng float64, seq int64) (accepted bool, cell string, reason string, err error)
}
type DispatchClient interface {
	FindCandidates(ctx context.Context, lat, lng, radiusM float64, max int) ([]map[string]any, int, int64, error)
}

// NewServer constructs the gateway.
func NewServer(cfg config.Config, log *slog.Logger, store *redis.Store,
	tokens *auth.TokenManager, hub *Hub, common *metrics.Common,
	drv DriverClient, rd RideClient, loc LocationClient, dsp DispatchClient) *Server {
	return &Server{
		cfg: cfg, log: log, tokens: tokens, store: store, hub: hub,
		common: common, driverClient: drv, rideClient: rd, locClient: loc, dispatch: dsp,
	}
}

// Routes builds the HTTP mux with all public API routes.
func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()

	// Public (no auth).
	mux.HandleFunc("POST /api/v1/auth/login", s.handleLogin)
	mux.HandleFunc("GET /health", s.handleHealth)
	mux.HandleFunc("GET /ready", s.handleReady)
	mux.HandleFunc("GET /metrics", s.handleMetrics)

	// Authenticated (JWT).
	mux.Handle("POST /api/v1/drivers", s.requireAuth(auth.RoleDriver, auth.RoleAdmin, s.handleRegisterDriver))
	mux.Handle("PATCH /api/v1/drivers/{id}/status", s.requireAuth(auth.RoleDriver, auth.RoleAdmin, s.handleDriverStatus))
	mux.Handle("POST /api/v1/drivers/{id}/location", s.requireAuth(auth.RoleDriver, auth.RoleAdmin, s.handleDriverLocation))
	mux.Handle("POST /api/v1/rides", s.requireAuth(auth.RoleUser, auth.RoleAdmin, s.handleCreateRide))
	mux.Handle("GET /api/v1/rides/{id}", s.requireAuth(auth.RoleUser, auth.RoleAdmin, s.handleGetRide))
	mux.Handle("POST /api/v1/rides/{id}/cancel", s.requireAuth(auth.RoleUser, auth.RoleAdmin, s.handleCancelRide))
	mux.Handle("GET /api/v1/drivers/nearby", s.requireAuth(auth.RoleUser, auth.RoleAdmin, s.handleNearbyDrivers))
	mux.Handle("GET /api/v1/ws", s.requireAuth(auth.RoleUser, auth.RoleDriver, auth.RoleAdmin, s.handleWS))

	return s.middleware(mux)
}

// middleware wraps the mux with request-id, rate limiting, metrics, and
// secure headers.
func (s *Server) middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		// Request ID.
		rid := r.Header.Get("X-Request-Id")
		if rid == "" {
			rid = "req-" + time.Now().Format("20060102150405.000000000")
		}
		r.Header.Set("X-Request-Id", rid)
		w.Header().Set("X-Request-Id", rid)

		// Secure headers.
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("Content-Security-Policy", "default-src 'none'")

		// Rate limiting (per-IP for unauthenticated; per-user after auth).
		ip := clientIP(r)
		if r.URL.Path != "/health" && r.URL.Path != "/metrics" {
			allowed, err := s.store.RateAllow(r.Context(), "ip", ip, s.cfg.IPRateCapacity, s.cfg.IPRateRefill, 1)
			if err == nil && !allowed {
				s.writeError(w, http.StatusTooManyRequests, "rate_limited", "too many requests")
				return
			}
		}

		rw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rw, r)

		route := r.Method + " " + r.URL.Path
		if s.common != nil {
			s.common.RequestsTotal.WithLabelValues(route, http.StatusText(rw.status)).Inc()
			s.common.RequestLatency.WithLabelValues(route).Observe(time.Since(start).Seconds())
		}
	})
}

// requireAuth verifies the JWT and enforces roles, then calls next with the
// claims in context.
func (s *Server) requireAuth(roles []auth.Role, next http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tok := extractToken(r)
		if tok == "" {
			s.writeError(w, http.StatusUnauthorized, "missing_token", "Authorization: Bearer <token> required")
			return
		}
		claims, err := s.tokens.Verify(tok)
		if err != nil {
			s.writeError(w, http.StatusUnauthorized, "invalid_token", "token verification failed")
			return
		}
		if err := auth.RequireRole(auth.ContextWithClaims(r.Context(), claims), roles...); err != nil {
			s.writeError(w, http.StatusForbidden, "forbidden", "insufficient role")
			return
		}
		// Per-user rate limit (after auth, so we know the user).
		if allowed, err := s.store.RateAllow(r.Context(), "user", claims.UserID, s.cfg.UserRateCapacity, s.cfg.UserRateRefill, 1); err == nil && !allowed {
			s.writeError(w, http.StatusTooManyRequests, "rate_limited", "user rate limit exceeded")
			return
		}
		next(w, r.WithContext(auth.ContextWithClaims(r.Context(), claims)))
	})
}

// --- Handlers ---

// handleLogin issues a JWT. In production this would check credentials
// against the users table; here it accepts a known dev identity.
func (s *Server) handleLogin(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Email string `json:"email"`
		Role  string `json:"role"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64*1024)).Decode(&body); err != nil {
		s.writeError(w, http.StatusBadRequest, "bad_request", "invalid JSON")
		return
	}
	if body.Email == "" {
		s.writeError(w, http.StatusBadRequest, "bad_request", "email is required")
		return
	}
	role := auth.Role(body.Role)
	switch role {
	case auth.RoleUser, auth.RoleDriver, auth.RoleAdmin:
	default:
		role = auth.RoleUser
	}
	tok, err := s.tokens.Issue("user-"+body.Email, role)
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, "internal", "could not issue token")
		return
	}
	s.writeJSON(w, http.StatusOK, map[string]any{
		"token":      tok,
		"role":       string(role),
		"expires_in": int(s.cfg.JWTExpiry.Seconds()),
	})
}

// handleRegisterDriver registers a driver (DRIVER/ADMIN).
func (s *Server) handleRegisterDriver(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Name    string  `json:"name"`
		Email   string  `json:"email"`
		Lat     float64 `json:"lat"`
		Lng     float64 `json:"lng"`
		Vehicle struct {
			Make  string `json:"make"`
			Model string `json:"model"`
			Plate string `json:"plate"`
			Seats int    `json:"seats"`
		} `json:"vehicle"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64*1024)).Decode(&body); err != nil {
		s.writeError(w, http.StatusBadRequest, "bad_request", "invalid JSON")
		return
	}
	id, state, err := s.driverClient.RegisterDriver(r.Context(), body.Name, body.Email,
		body.Vehicle.Make, body.Vehicle.Model, body.Vehicle.Plate, body.Vehicle.Seats, body.Lat, body.Lng)
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	s.writeJSON(w, http.StatusCreated, map[string]any{"driver_id": id, "state": state})
}

// handleDriverStatus transitions a driver's state (DRIVER/ADMIN).
func (s *Server) handleDriverStatus(w http.ResponseWriter, r *http.Request) {
	driverID := r.PathValue("id")
	var body struct {
		State string `json:"state"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64*1024)).Decode(&body); err != nil {
		s.writeError(w, http.StatusBadRequest, "bad_request", "invalid JSON")
		return
	}
	prev, cur, err := s.driverClient.UpdateDriverStatus(r.Context(), driverID, body.State)
	if err != nil {
		s.writeError(w, http.StatusUnprocessableEntity, "invalid_transition", err.Error())
		return
	}
	s.writeJSON(w, http.StatusOK, map[string]any{"previous": prev, "current": cur})
}

// handleDriverLocation ingests a GPS update (DRIVER/ADMIN).
func (s *Server) handleDriverLocation(w http.ResponseWriter, r *http.Request) {
	driverID := r.PathValue("id")
	var body struct {
		Lat  float64 `json:"lat"`
		Lng  float64 `json:"lng"`
		Seq  int64   `json:"seq"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64*1024)).Decode(&body); err != nil {
		s.writeError(w, http.StatusBadRequest, "bad_request", "invalid JSON")
		return
	}
	accepted, cell, reason, err := s.locClient.UpdateLocation(r.Context(), driverID, body.Lat, body.Lng, body.Seq)
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	if !accepted {
		s.writeJSON(w, http.StatusConflict, map[string]any{"accepted": false, "reason": reason})
		return
	}
	s.writeJSON(w, http.StatusOK, map[string]any{"accepted": true, "h3_cell": cell})
}

// handleCreateRide creates a ride (USER/ADMIN) and triggers dispatch.
func (s *Server) handleCreateRide(w http.ResponseWriter, r *http.Request) {
	userID := auth.UserIDFromContext(r.Context())
	var body struct {
		PickupLat  float64 `json:"pickup_lat"`
		PickupLng  float64 `json:"pickup_lng"`
		DropoffLat float64 `json:"dropoff_lat"`
		DropoffLng float64 `json:"dropoff_lng"`
		RadiusM    int     `json:"radius_m"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64*1024)).Decode(&body); err != nil {
		s.writeError(w, http.StatusBadRequest, "bad_request", "invalid JSON")
		return
	}
	rideID, err := s.rideClient.CreateRide(r.Context(), userID, body.PickupLat, body.PickupLng, body.DropoffLat, body.DropoffLng, body.RadiusM)
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	s.writeJSON(w, http.StatusCreated, map[string]any{
		"ride_id": rideID, "state": "REQUESTED",
		"message": "ride requested; dispatch engine will assign a driver",
	})
}

// handleGetRide fetches a ride (USER/ADMIN).
func (s *Server) handleGetRide(w http.ResponseWriter, r *http.Request) {
	rideID := r.PathValue("id")
	ride, err := s.rideClient.GetRide(r.Context(), rideID)
	if err != nil {
		s.writeError(w, http.StatusNotFound, "not_found", "ride not found")
		return
	}
	s.writeJSON(w, http.StatusOK, ride)
}

// handleCancelRide cancels a ride (USER/ADMIN).
func (s *Server) handleCancelRide(w http.ResponseWriter, r *http.Request) {
	rideID := r.PathValue("id")
	userID := auth.UserIDFromContext(r.Context())
	var body struct {
		Reason string `json:"reason"`
	}
	_ = json.NewDecoder(http.MaxBytesReader(w, r.Body, 64*1024)).Decode(&body)
	ride, err := s.rideClient.CancelRide(r.Context(), rideID, userID, body.Reason)
	if err != nil {
		s.writeError(w, http.StatusUnprocessableEntity, "cannot_cancel", err.Error())
		return
	}
	s.writeJSON(w, http.StatusOK, ride)
}

// handleNearbyDrivers lists nearby available drivers (USER/ADMIN).
func (s *Server) handleNearbyDrivers(w http.ResponseWriter, r *http.Request) {
	lat := queryFloat(r, "lat")
	lng := queryFloat(r, "lng")
	radius := queryInt(r, "radius_m", 1000)
	max := queryInt(r, "limit", 20)
	cands, cells, tookNs, err := s.dispatch.FindCandidates(r.Context(), lat, lng, float64(radius), max)
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	s.writeJSON(w, http.StatusOK, map[string]any{
		"candidates": cands, "cells_searched": cells, "took_ns": tookNs,
	})
}

// handleWS upgrades to WebSocket (auth via ?token= or Authorization header).
func (s *Server) handleWS(w http.ResponseWriter, r *http.Request) {
	user := auth.UserIDFromContext(r.Context())
	role := string(auth.RoleFromContext(r.Context()))
	s.hub.ServeWS(w, r, user, role)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	s.writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "service": "gateway"})
}
func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	if err := s.store.Ping(r.Context()); err != nil {
		s.writeError(w, http.StatusServiceUnavailable, "not_ready", "redis unavailable")
		return
	}
	s.writeJSON(w, http.StatusOK, map[string]any{"status": "ready"})
}
func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	// Delegates to the metrics handler mounted by the server bootstrap.
	http.NotFoundHandler().ServeHTTP(w, r)
}

// --- helpers ---

func (s *Server) writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func (s *Server) writeError(w http.ResponseWriter, code int, errCode, msg string) {
	s.writeJSON(w, code, map[string]any{"error": errCode, "message": msg})
}

func extractToken(r *http.Request) string {
	if h := r.Header.Get("Authorization"); strings.HasPrefix(h, "Bearer ") {
		return strings.TrimPrefix(h, "Bearer ")
	}
	return r.URL.Query().Get("token")
}

func clientIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		return strings.Split(xff, ",")[0]
	}
	host := r.RemoteAddr
	if i := strings.LastIndex(host, ":"); i > 0 {
		return host[:i]
	}
	return host
}

func queryFloat(r *http.Request, key string) float64 {
	var v float64
	if s := r.URL.Query().Get(key); s != "" {
		json.Unmarshal([]byte(s), &v)
	}
	return v
}

func queryInt(r *http.Request, key string, def int) int {
	var v int
	if s := r.URL.Query().Get(key); s != "" {
		json.Unmarshal([]byte(s), &v)
	}
	if v == 0 {
		return def
	}
	return v
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}