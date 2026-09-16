// Package events defines the domain events flowing through Kafka.
//
// Every event is JSON-encoded, carries a monotonic event_id for idempotency,
// and includes the traceparent header (when tracing is enabled) so consumers
// can continue the trace.
package events

import (
	"encoding/json"
	"fmt"
	"time"
)

// Envelope is the common wrapper for all events.
type Envelope struct {
	EventID   string    `json:"event_id"`
	Type      string    `json:"type"`
	Timestamp time.Time `json:"timestamp"`
	TraceID   string    `json:"trace_id,omitempty"`
	RequestID string    `json:"request_id,omitempty"`
}

// LocationUpdate is a driver GPS update (topic: driver-location, key: driver_id).
type LocationUpdate struct {
	Envelope
	DriverID  string  `json:"driver_id"`
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
	// Sequence is the per-driver monotonic ordering field. Consumers reject
	// updates with sequence <= last-applied (out-of-order / duplicate guard).
	Sequence int64 `json:"sequence"`
	H3Cell   string `json:"h3_cell"`
}

// RideRequested is a new ride request (topic: ride-requested, key: ride_id).
type RideRequested struct {
	Envelope
	RideID    string  `json:"ride_id"`
	UserID    string  `json:"user_id"`
	PickupLat float64 `json:"pickup_lat"`
	PickupLng float64 `json:"pickup_lng"`
	DropoffLat float64 `json:"dropoff_lat"`
	DropoffLng float64 `json:"dropoff_lng"`
	RadiusM   int32   `json:"radius_m"`
}

// DriverReserved is emitted when a driver lease is won (topic: driver-reserved, key: driver_id).
type DriverReserved struct {
	Envelope
	DriverID string `json:"driver_id"`
	RideID   string `json:"ride_id"`
	LeaseVersion uint64 `json:"lease_version"`
}

// DispatchCreated is emitted after a dispatch record is created
// (topic: dispatch-created, key: ride_id).
type DispatchCreated struct {
	Envelope
	DispatchID string  `json:"dispatch_id"`
	RideID     string  `json:"ride_id"`
	DriverID   string  `json:"driver_id"`
	DistanceM  float64 `json:"distance_m"`
	Score      float64 `json:"score"`
}

// RideStatus is a ride lifecycle transition (topic: ride-status, key: ride_id).
type RideStatus struct {
	Envelope
	RideID  string `json:"ride_id"`
	State   string `json:"state"`
	DriverID string `json:"driver_id,omitempty"`
	Reason  string `json:"reason,omitempty"`
}

// DriverStatus is a driver availability transition (topic: driver-status, key: driver_id).
type DriverStatus struct {
	Envelope
	DriverID string `json:"driver_id"`
	State    string `json:"state"`
	Reason   string `json:"reason,omitempty"`
}

// Encode serializes an event to JSON.
func Encode(v any) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("event: encode: %w", err)
	}
	return b, nil
}

// DecodeLocationUpdate parses a driver-location message.
func DecodeLocationUpdate(b []byte) (*LocationUpdate, error) {
	var e LocationUpdate
	if err := json.Unmarshal(b, &e); err != nil {
		return nil, fmt.Errorf("event: decode location: %w", err)
	}
	return &e, nil
}

// DecodeRideRequested parses a ride-requested message.
func DecodeRideRequested(b []byte) (*RideRequested, error) {
	var e RideRequested
	if err := json.Unmarshal(b, &e); err != nil {
		return nil, fmt.Errorf("event: decode ride-requested: %w", err)
	}
	return &e, nil
}

// DecodeDispatchCreated parses a dispatch-created message.
func DecodeDispatchCreated(b []byte) (*DispatchCreated, error) {
	var e DispatchCreated
	if err := json.Unmarshal(b, &e); err != nil {
		return nil, fmt.Errorf("event: decode dispatch-created: %w", err)
	}
	return &e, nil
}

// DecodeRideStatus parses a ride-status message.
func DecodeRideStatus(b []byte) (*RideStatus, error) {
	var e RideStatus
	if err := json.Unmarshal(b, &e); err != nil {
		return nil, fmt.Errorf("event: decode ride-status: %w", err)
	}
	return &e, nil
}

// DecodeDriverStatus parses a driver-status message.
func DecodeDriverStatus(b []byte) (*DriverStatus, error) {
	var e DriverStatus
	if err := json.Unmarshal(b, &e); err != nil {
		return nil, fmt.Errorf("event: decode driver-status: %w", err)
	}
	return &e, nil
}