// Package redis implements the hot real-time state layer for NEXUS-DISPATCH.
//
// Redis holds loss-tolerant, sub-millisecond state:
//   - driver last-known position + H3 cell membership
//   - driver availability state
//   - reservation leases (the no-double-booking primitive)
//   - token-bucket rate limiting
//
// Every multi-key mutation is a Lua script so it runs atomically in Redis.
// See docs/decisions/ADR-003 and ADR-006 for the design rationale.
package redis

import (
	"fmt"
	"strconv"
	"strings"
)

const (
	// DriverKeyPrefix is the per-driver profile/state hash.
	DriverKeyPrefix = "driver:"
	// LocKeySuffix is the per-driver last-position hash suffix.
	LocKeySuffix = ":loc"
	// LeaseKeySuffix is the per-driver reservation lease suffix.
	LeaseKeySuffix = ":lease"
	// SeqKeyPrefix is the per-driver monotonic location-sequence guard.
	SeqKeyPrefix = "seq:"
	// CellKeyPrefix is the H3 cell → driver-set mapping.
	CellKeyPrefix = "h3:"
	// CellKeySuffix terminates a cell key.
	CellKeySuffix = ":drivers"
	// GeoKey is the global geohash zset (fallback + precise distance).
	GeoKey = "geo:drivers"
	// RideKeyPrefix is the hot ride-state hash.
	RideKeyPrefix = "ride:"
	// RateLimitPrefix is the token-bucket key prefix.
	RateLimitPrefix = "ratelimit:"
)

// DriverKey returns the driver profile/state hash key.
func DriverKey(id string) string { return DriverKeyPrefix + id }

// DriverLocKey returns the driver last-position hash key.
func DriverLocKey(id string) string { return DriverKeyPrefix + id + LocKeySuffix }

// DriverLeaseKey returns the driver reservation lease key.
func DriverLeaseKey(id string) string { return DriverKeyPrefix + id + LeaseKeySuffix }

// DriverSeqKey returns the driver location-sequence guard key.
func DriverSeqKey(id string) string { return SeqKeyPrefix + id }

// CellKey returns the H3 cell driver-set key.
func CellKey(cell string) string { return CellKeyPrefix + cell + CellKeySuffix }

// RideKey returns the hot ride-state hash key.
func RideKey(id string) string { return RideKeyPrefix + id }

// RateLimitKey returns the token-bucket key for a scope+id.
func RateLimitKey(scope, id string) string {
	return RateLimitPrefix + scope + ":" + id
}

// LeaseValue encodes the reservation lease payload:
//
//	ride_id:token:version
//
// The token makes renewal re-entrant (only the winner can extend), and the
// version makes release a compare-and-delete (a stale holder with an old
// version cannot delete a newer lease).
type LeaseValue struct {
	RideID   string
	Token    string
	Version  uint64
}

func (l LeaseValue) String() string {
	return fmt.Sprintf("%s:%s:%d", l.RideID, l.Token, l.Version)
}

// ParseLeaseValue parses a lease string back into its components.
func ParseLeaseValue(s string) (LeaseValue, error) {
	var l LeaseValue
	// Split on ':' — rideID and token never contain ':'.
	parts := strings.Split(s, ":")
	if len(parts) != 3 {
		return l, fmt.Errorf("redis: malformed lease value %q", s)
	}
	l.RideID, l.Token = parts[0], parts[1]
	v, err := strconv.ParseUint(parts[2], 10, 64)
	if err != nil {
		return l, fmt.Errorf("redis: malformed lease version in %q: %w", s, err)
	}
	l.Version = v
	return l, nil
}