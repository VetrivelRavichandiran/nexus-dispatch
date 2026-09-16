// Package geospatial provides H3-based spatial partitioning and geodesic
// distance calculations for NEXUS-DISPATCH.
//
// Design notes:
//   - We use H3 resolution 7 cells (~1.2 km², circumradius ≈ 400 m) as the
//     primary spatial partition. A search of radius R meters expands a
//     k-ring where k = ceil(R / cellCircumradius) + 1, so the candidate set is
//     bounded by fleet density × cells, NOT by fleet size.
//   - Distances are GEODESIC (haversine). They are NOT road distances; the
//     dispatch scorer isolates the distance term so a routing engine can be
//     swapped in later (see docs/decisions/ADR-004).
//   - The H3 implementation is github.com/dimchansky/h3-go (pure Go, no cgo).
//     Parity with the official H3 reference is pinned in parity_test.go.
package geospatial

import (
	"fmt"
	"math"

	h3 "github.com/dimchansky/h3-go"
)

// DefaultResolution is the H3 resolution used for the driver index.
const DefaultResolution = 7

// cellCircumradiusM is the maximum distance from a res-7 cell center to its
// farthest point (≈ 400 m). Used to size the k-ring for a given radius.
const cellCircumradiusM = 400.0

// CellKey returns the hex string for the H3 cell containing (lat, lng) at the
// given resolution.
func CellKey(lat, lng float64, res int) (string, error) {
	cell, err := h3.LatLngToCell(h3.LatLngDegs(lat, lng), res)
	if err != nil {
		return "", fmt.Errorf("geospatial: latLngToCell(%f, %f, %d): %w", lat, lng, res, err)
	}
	return cell.String(), nil
}

// CellCenter returns the geographic center of an H3 cell.
func CellCenter(cellHex string) (lat, lng float64, err error) {
	cell, err := h3.ParseCell(cellHex)
	if err != nil {
		return 0, 0, fmt.Errorf("geospatial: parse cell %q: %w", cellHex, err)
	}
	ll, err := cell.LatLng()
	if err != nil {
		return 0, 0, fmt.Errorf("geospatial: cell center %q: %w", cellHex, err)
	}
	return float64(ll.Lat) / h3.RadPerDeg, float64(ll.Lng) / h3.RadPerDeg, nil
}

// Ring returns the k-ring of cells around origin (origin included), i.e.
// 3k(k+1)+1 cells. k=0 → 1 cell, k=1 → 7, k=2 → 19.
func Ring(originHex string, k int) ([]string, error) {
	if k < 0 {
		return nil, fmt.Errorf("geospatial: negative ring %d", k)
	}
	cell, err := h3.ParseCell(originHex)
	if err != nil {
		return nil, fmt.Errorf("geospatial: parse cell %q: %w", originHex, err)
	}
	ring, err := cell.GridDisk(k)
	if err != nil {
		return nil, fmt.Errorf("geospatial: gridDisk(%s, %d): %w", originHex, k, err)
	}
	out := make([]string, len(ring))
	for i, c := range ring {
		out[i] = c.String()
	}
	return out, nil
}

// RingForRadius computes the minimum ring k that guarantees coverage of a
// search radius (meters) around any point in the origin cell: every point
// within radius of the origin lies in a cell at grid distance ≤ k.
func RingForRadius(radiusM float64, res int) int {
	// Conservative: cell circumradius at res 7 ≈ 400 m (res-independent
	// approximation is fine because res is fixed by deployment config).
	k := int(math.Ceil(radiusM/cellCircumradiusM))
	if k < 1 {
		k = 1
	}
	return k
}

// HaversineMeters returns the great-circle distance between two points in
// meters. This is a GEODESIC distance, not a road distance.
func HaversineMeters(lat1, lng1, lat2, lng2 float64) float64 {
	const r = 6371008.8 // mean Earth radius (m), IUGG
	phi1, phi2 := lat1*math.Pi/180, lat2*math.Pi/180
	dphi := (lat2 - lat1) * math.Pi / 180
	dlam := (lng2 - lng1) * math.Pi / 180
	a := math.Sin(dphi/2)*math.Sin(dphi/2) +
		math.Cos(phi1)*math.Cos(phi2)*math.Sin(dlam/2)*math.Sin(dlam/2)
	return 2 * r * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))
}

// ValidateLatLng reports whether (lat, lng) is a plausible Earth coordinate.
func ValidateLatLng(lat, lng float64) error {
	if math.IsNaN(lat) || math.IsNaN(lng) {
		return fmt.Errorf("geospatial: NaN coordinate")
	}
	if lat < -90 || lat > 90 {
		return fmt.Errorf("geospatial: latitude %f out of range [-90, 90]", lat)
	}
	if lng < -180 || lng > 180 {
		return fmt.Errorf("geospatial: longitude %f out of range [-180, 180]", lng)
	}
	return nil
}

// SearchPlan describes how to search for drivers around a point: the origin
// cell and the ring of cells to scan.
type SearchPlan struct {
	OriginCell string
	Cells      []string // origin + ring
	RingK      int
	RadiusM    float64
}

// PlanSearch builds a SearchPlan for (lat, lng, radiusM).
func PlanSearch(lat, lng float64, radiusM float64, res int) (*SearchPlan, error) {
	origin, err := CellKey(lat, lng, res)
	if err != nil {
		return nil, err
	}
	k := RingForRadius(radiusM, res)
	cells, err := Ring(origin, k)
	if err != nil {
		return nil, err
	}
	return &SearchPlan{OriginCell: origin, Cells: cells, RingK: k, RadiusM: radiusM}, nil
}