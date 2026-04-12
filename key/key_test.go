package key

import "testing"

func TestFromJSONReturnsErrorForUnsupportedValues(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		v    any
	}{
		{
			name: "chan",
			v: struct {
				C chan int `json:"c"`
			}{C: make(chan int)},
		},
		{
			name: "func",
			v: struct {
				F func() `json:"f"`
			}{F: func() {}},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			k, err := FromJSON(tt.v)
			if err == nil {
				t.Fatalf("FromJSON() expected error for %s", tt.name)
			}
			if k != "" {
				t.Fatalf("FromJSON() key = %q, want empty key on error", k)
			}
		})
	}
}

func TestMustFromJSONFallbackCollidesWithEmptyPayload(t *testing.T) {
	t.Parallel()

	invalidA := struct {
		C chan int `json:"c"`
	}{C: make(chan int)}
	invalidB := struct {
		F func() `json:"f"`
	}{F: func() {}}

	emptyPayloadKey := FromParts("")
	if got := MustFromJSON(invalidA); got != emptyPayloadKey {
		t.Fatalf("MustFromJSON(chan) = %q, want %q", got, emptyPayloadKey)
	}
	if got := MustFromJSON(invalidB); got != emptyPayloadKey {
		t.Fatalf("MustFromJSON(func) = %q, want %q", got, emptyPayloadKey)
	}

	// New FromJSON API surfaces errors so callers can avoid silent collisions.
	if _, err := FromJSON(invalidA); err == nil {
		t.Fatal("FromJSON(chan) expected error")
	}
	if _, err := FromJSON(invalidB); err == nil {
		t.Fatal("FromJSON(func) expected error")
	}
}
