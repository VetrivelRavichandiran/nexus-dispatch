// Package driver implements the Driver Service: registration, profiles, and
// the driver availability state machine.
package driver

import (
	"fmt"
)

// State is a driver availability state.
type State string

const (
	StateOffline  State = "OFFLINE"
	StateAvailable State = "AVAILABLE"
	StateReserved State = "RESERVED"
	StateOnTrip   State = "ON_TRIP"
	StatePaused   State = "PAUSED"
)

// AllStates lists every valid state.
var AllStates = []State{StateOffline, StateAvailable, StateReserved, StateOnTrip, StatePaused}

// validTransitions encodes the allowed state machine edges.
//
//	OFFLINE  → AVAILABLE, PAUSED
//	AVAILABLE → RESERVED, OFFLINE, PAUSED
//	RESERVED → ON_TRIP, AVAILABLE (release), OFFLINE, PAUSED
//	ON_TRIP  → AVAILABLE (trip end), OFFLINE
//	PAUSED   → AVAILABLE, OFFLINE
var validTransitions = map[State]map[State]bool{
	StateOffline:   {StateAvailable: true, StatePaused: true},
	StateAvailable: {StateReserved: true, StateOffline: true, StatePaused: true},
	StateReserved:  {StateOnTrip: true, StateAvailable: true, StateOffline: true, StatePaused: true},
	StateOnTrip:    {StateAvailable: true, StateOffline: true},
	StatePaused:    {StateAvailable: true, StateOffline: true},
}

// CanTransition reports whether from→to is a legal state-machine edge.
// A no-op transition (from==to) is allowed (idempotent).
func CanTransition(from, to State) bool {
	if from == to {
		return true
	}
	return validTransitions[from][to]
}

// ValidateState reports whether s is a known state.
func ValidateState(s State) error {
	for _, v := range AllStates {
		if v == s {
			return nil
		}
	}
	return fmt.Errorf("driver: unknown state %q", s)
}

// AssertTransition validates a transition and returns a descriptive error.
func AssertTransition(from, to State) error {
	if err := ValidateState(to); err != nil {
		return err
	}
	if !CanTransition(from, to) {
		return fmt.Errorf("driver: illegal transition %s → %s", from, to)
	}
	return nil
}