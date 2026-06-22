package livekit

import (
	"math"
	"testing"
)

func TestSoftLimit_BelowThreshold(t *testing.T) {
	tests := []int16{0, 1, -1, 100, -100, 8192, -8192}
	for _, input := range tests {
		got := softLimit(input)
		if got != input {
			t.Errorf("softLimit(%d) = %d, want %d (below threshold should pass through)", input, got, input)
		}
	}
}

func TestSoftLimit_AboveThresholdCompresses(t *testing.T) {
	input := int16(16000)
	got := softLimit(input)

	if got <= 8192 {
		t.Fatalf("softLimit(%d) = %d, should stay above threshold", input, got)
	}
	if got >= input {
		t.Fatalf("softLimit(%d) = %d, should be compressed below input", input, got)
	}

	excess := int32(16000) - 8192
	expected := int16(8192 + excess/10)
	if got != expected {
		t.Errorf("softLimit(%d) = %d, want %d (8192 + excess/10)", input, got, expected)
	}
}

func TestSoftLimit_SymmetricPositiveNegative(t *testing.T) {
	tests := []int16{10000, 16000, 20000, 30000}
	for _, input := range tests {
		pos := softLimit(input)
		neg := softLimit(-input)
		if pos != -neg {
			t.Errorf("softLimit(%d)=%d but softLimit(%d)=%d — not symmetric", input, pos, -input, neg)
		}
	}
}

func TestSoftLimit_PreventsHardClip(t *testing.T) {
	for i := -32768; i <= 32767; i += 64 {
		input := int16(i)
		got := softLimit(input)
		if got > 32767 {
			t.Fatalf("softLimit(%d) = %d exceeds int16 max", input, got)
		}
		if got < -32768 {
			t.Fatalf("softLimit(%d) = %d below int16 min", input, got)
		}
	}
}

func TestSoftLimit_MixerSumScenario(t *testing.T) {
	speaker1 := int16(20000)
	speaker2 := int16(20000)
	mixed := int32(speaker1) + int32(speaker2)
	if mixed > math.MaxInt16 {
		mixed = math.MaxInt16
	}
	limited := softLimit(int16(mixed))

	if limited == math.MaxInt16 {
		t.Error("softLimit produced hard clip (32767) — should compress instead")
	}
	if limited <= 8192 {
		t.Error("softLimit collapsed signal below threshold — excessive compression")
	}
}
