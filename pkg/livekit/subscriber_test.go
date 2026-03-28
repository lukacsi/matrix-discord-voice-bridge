package livekit

import (
	"math"
	"testing"
)

func TestSoftLimit(t *testing.T) {
	tests := []struct {
		name  string
		input int16
	}{
		{"zero", 0},
		{"quiet positive", 1000},
		{"quiet negative", -1000},
		{"at threshold", 8192},
		{"above threshold", 25000},
		{"max int16", math.MaxInt16},
		{"min int16", math.MinInt16 + 1},
		{"min int16 exact", math.MinInt16},
		{"near max", 30000},
		{"near min", -30000},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := softLimit(tt.input)

			// Output must never exceed int16 range
			if got > math.MaxInt16 || got < math.MinInt16 {
				t.Errorf("softLimit(%d) = %d; exceeds int16 range", tt.input, got)
			}

			// Output must preserve sign
			if (tt.input > 0 && got < 0) || (tt.input < 0 && got > 0) {
				t.Errorf("softLimit(%d) = %d; sign changed", tt.input, got)
			}

			// Below threshold: output == input
			if tt.input >= -8192 && tt.input <= 8192 {
				if got != tt.input {
					t.Errorf("softLimit(%d) = %d; want %d (below threshold)", tt.input, got, tt.input)
				}
			}

			// Above threshold: output must be less than input (compressed)
			if tt.input > 8192 && got >= tt.input {
				t.Errorf("softLimit(%d) = %d; expected compression", tt.input, got)
			}
			if tt.input < -8192 && got <= tt.input {
				t.Errorf("softLimit(%d) = %d; expected compression", tt.input, got)
			}
		})
	}
}

func TestSoftLimitMonotonic(t *testing.T) {
	// Output should be monotonically increasing with input
	prev := softLimit(math.MinInt16 + 1)
	for i := int16(math.MinInt16 + 2); i < math.MaxInt16; i++ {
		got := softLimit(i)
		if got < prev {
			t.Fatalf("non-monotonic: softLimit(%d)=%d < softLimit(%d)=%d", i, got, i-1, prev)
		}
		prev = got
	}
}
