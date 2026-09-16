package geospatial

import (
	"testing"
)

// TestH3Parity pins our pure-Go H3 implementation against the OFFICIAL H3
// reference values (verified with the official H3 Python/JS libraries).
// If the vendored library ever drifts from the H3 spec, this test fails.
func TestH3Parity(t *testing.T) {
	cases := []struct {
		lat, lng float64
		res      int
		wantCell string
	}{
		{12.9716, 77.5946, 7, "8760145b4ffffff"}, // Bengaluru
		{37.7749, -122.4194, 7, "872830828ffffff"}, // San Francisco
		{40.7128, -74.0060, 7, "872a1072cffffff"}, // New York
		{0.0, 0.0, 7, "87754e64dffffff"}, // null island
	}
	for _, tc := range cases {
		got, err := CellKey(tc.lat, tc.lng, tc.res)
		if err != nil {
			t.Fatalf("CellKey(%v, %v, %d): %v", tc.lat, tc.lng, tc.res, err)
		}
		if got != tc.wantCell {
			t.Errorf("CellKey(%v, %v, %d) = %s, want %s", tc.lat, tc.lng, tc.res, got, tc.wantCell)
		}
	}
}

// TestH3CellCenterParity checks cell centers against official reference values.
func TestH3CellCenterParity(t *testing.T) {
	lat, lng, err := CellCenter("8760145b4ffffff")
	if err != nil {
		t.Fatal(err)
	}
	// Official H3: cell_to_latlng("8760145b4ffffff") = (12.9784984385692, 77.59039730205839)
	if d := abs(lat - 12.9784984385692); d > 1e-6 {
		t.Errorf("center lat = %f, want 12.9784984385692 (delta %f)", lat, d)
	}
	if d := abs(lng - 77.59039730205839); d > 1e-6 {
		t.Errorf("center lng = %f, want 77.59039730205839 (delta %f)", lng, d)
	}
}

func abs(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}

// TestRingSizes verifies gridDisk cardinalities: 3k(k+1)+1.
func TestRingSizes(t *testing.T) {
	want := map[int]int{0: 1, 1: 7, 2: 19, 3: 37, 5: 91}
	for k, n := range want {
		cells, err := Ring("8760145b4ffffff", k)
		if err != nil {
			t.Fatalf("Ring(k=%d): %v", k, err)
		}
		if len(cells) != n {
			t.Errorf("Ring(k=%d) returned %d cells, want %d", k, len(cells), n)
		}
		// origin must be included
		found := false
		for _, c := range cells {
			if c == "8760145b4ffffff" {
				found = true
			}
		}
		if !found {
			t.Errorf("Ring(k=%d) does not contain origin", k)
		}
	}
}

// TestHaversineKnownDistances pins geodesic math against known values.
func TestHaversineKnownDistances(t *testing.T) {
	// 1 degree of latitude ≈ 111.19 km (mean Earth radius).
	d := HaversineMeters(0, 0, 1, 0)
	if d < 111000 || d > 111400 {
		t.Errorf("1° latitude = %.0f m, want ~111190 m", d)
	}
	// Antipodal points: half circumference ≈ 20015 km.
	d = HaversineMeters(0, 0, 0, 180)
	if d < 20000000 || d > 20030000 {
		t.Errorf("antipodal = %.0f m, want ~20015000 m", d)
	}
	// Same point.
	if d = HaversineMeters(12.9716, 77.5946, 12.9716, 77.5946); d != 0 {
		t.Errorf("same point distance = %f, want 0", d)
	}
	// Symmetry.
	a := HaversineMeters(12.9716, 77.5946, 12.98, 77.60)
	b := HaversineMeters(12.98, 77.60, 12.9716, 77.5946)
	if a != b {
		t.Errorf("haversine not symmetric: %f vs %f", a, b)
	}
}

// TestValidateLatLng covers the input-validation contract.
func TestValidateLatLng(t *testing.T) {
	valid := [][2]float64{{0, 0}, {90, 180}, {-90, -180}, {12.9716, 77.5946}}
	for _, p := range valid {
		if err := ValidateLatLng(p[0], p[1]); err != nil {
			t.Errorf("ValidateLatLng(%v) unexpected error: %v", p, err)
		}
	}
	invalid := [][2]float64{{91, 0}, {-91, 0}, {0, 181}, {0, -181}}
	for _, p := range invalid {
		if err := ValidateLatLng(p[0], p[1]); err == nil {
			t.Errorf("ValidateLatLng(%v) expected error", p)
		}
	}
}