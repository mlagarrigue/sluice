package sluice

import "testing"

func TestOverflowString(t *testing.T) {
	tests := []struct {
		policy Overflow
		want   string
	}{
		{DropOldest, "DropOldest"},
		{DropNewest, "DropNewest"},
		{Fail, "Fail"},
		{Overflow(9), "Overflow(9)"},
	}
	for _, tc := range tests {
		t.Run(tc.want, func(t *testing.T) {
			if got := tc.policy.String(); got != tc.want {
				t.Errorf("String = %q, want %q", got, tc.want)
			}
		})
	}
}
