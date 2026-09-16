package geospatial

import (
	"math"
	"testing"
)

// TestCellBoundaryNoMiss is the correctness test for spatial partitioning:
// for a request point, EVERY point within the search radius must lie in a
// cell that the k-ring search covers. We sample a dense grid of points around
// the request and verify each is covered by the planned ring. If the ring
// sizing (RingForRadius) is ever too small, this test fails.
func TestCellBoundaryNoMiss(t *testing.T) {
	// Request point: Bengaluru city center.
	reqLat, reqLng := 12.9716, 77.5946
	const radiusM = 1000.0

	plan, err := PlanSearch(reqLat, reqLng, radiusM, DefaultResolution)
	if err != nil {
		t.Fatal(err)
	}
	covered := make(map[string]bool, len(plan.Cells))
	for _, c := range plan.Cells {
		covered[c] = true
	}

	// Dense sampling: every 50 m in a box around the request, keep points
	// within radius. 50 m << cell size (~400 m), so boundary crossings are
	// densely exercised.
	const step = 0.0002 // ~22 m
	halfBox := 0.0095 // ~1.05 km
	checked := 0
	for dLat := -halfBox; dLat <= halfBox; dLat += step {
		for dLng := -halfBox; dLng <= halfBox; dLng += step {
			lat, lng := reqLat+dLat, reqLng+dLng
			d := HaversineMeters(reqLat, reqLng, lat, lng)
			if d > radiusM {
				continue
			}
			checked++
			cell, err := CellKey(lat, lng, DefaultResolution)
			if err != nil {
				t.Fatal(err)
			}
			if !covered[cell] {
				t.Fatalf("point (%.6f, %.6f) at %.0f m from request is in cell %s, NOT covered by k=%d ring (%d cells)",
					lat, lng, d, cell, plan.RingK, len(plan.Cells))
			}
		}
	}
	if checked < 5000 {
		t.Fatalf("test too weak: only %d points checked", checked)
	}
	t.Logf("checked %d points within %.0f m; all covered by k=%d ring", checked, radiusM, plan.RingK)
}

// TestBoundaryDriverFound simulates the exact failure mode the spec calls out:
// a driver sitting just across a cell boundary from the request. The driver's
// cell differs from the request's cell, but the ring search must still find
// the candidate (which is then distance-filtered).
func TestBoundaryDriverFound(t *testing.T) {
	reqLat, reqLng := 12.9716, 77.5946
	reqCell, _ := CellKey(reqLat, reqLng, DefaultResolution)

	// Walk SOUTH from the request in 10 m steps until we cross into a
	// different cell (the request point is ~600 m north of the cell's south
	// edge), then stop — the driver ends up 600–610 m away, well within a
	// 1000 m radius.
	var driverLat, driverLng float64
	var driverCell string
	for dist := 10.0; dist <= 800.0; dist += 10.0 {
		driverLat = reqLat - dist/111320.0
		driverLng = reqLng
		driverCell, _ = CellKey(driverLat, driverLng, DefaultResolution)
		if driverCell != reqCell {
			break
		}
	}
	if driverCell == reqCell {
		t.Fatal("test setup failed: never crossed a cell boundary in 800 m south")
	}

	plan, err := PlanSearch(reqLat, reqLng, 1000.0, DefaultResolution)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, c := range plan.Cells {
		if c == driverCell {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("driver in adjacent cell %s (request cell %s) not covered by ring k=%d",
			driverCell, reqCell, plan.RingK)
	}
	// And the precise distance filter must keep the driver (300 m < 1000 m).
	d := HaversineMeters(reqLat, reqLng, driverLat, driverLng)
	if d > 1000.0 {
		t.Fatalf("driver at %f m should pass a 1000 m filter", d)
	}
}

// TestRingForRadiusMonotonic verifies the radius→k mapping is sane.
func TestRingForRadiusMonotonic(t *testing.T) {
	prev := 0
	for _, r := range []float64{100, 400, 800, 1000, 2000, 5000, 10000} {
		k := RingForRadius(r, DefaultResolution)
		if k < prev {
			t.Errorf("RingForRadius not monotonic: r=%f → k=%d (prev %d)", r, k, prev)
		}
		prev = k
		// invariant: k must cover the radius: (k) * cellCircumradius >= r - margin
		if float64(k)*cellCircumradiusM < r-400 {
			t.Errorf("RingForRadius(%f) = %d cannot cover radius", r, k)
		}
	}
	_ = math.Abs
}