package reservation

import "errors"

// ErrConflict is returned when the driver is already reserved by another ride.
var ErrConflict = errors.New("reservation: driver already reserved")

// ErrNotAvailable is returned when the driver is not in a reservable state.
var ErrNotAvailable = errors.New("reservation: driver not available")