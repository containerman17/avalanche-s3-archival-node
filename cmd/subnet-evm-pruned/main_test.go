//go:build subnetbench

package main

import (
	"encoding/json"
	"testing"

	"github.com/ava-labs/avalanchego/utils/constants"
)

func TestBackendConfig(t *testing.T) {
	for _, backend := range []string{"firewood", "epochdb", "rust"} {
		t.Run(backend, func(t *testing.T) {
			input := []byte(`{"benchmark-state-backend":"` + backend + `","state-scheme":"firewood","pruning-enabled":true,"state-sync-enabled":false,"snapshot-cache":0}`)
			got, stripped, err := backendConfig(input, constants.FujiID)
			if err != nil {
				t.Fatal(err)
			}
			if got != backend {
				t.Fatalf("backend = %q, want %q", got, backend)
			}
			var before, after map[string]json.RawMessage
			if err := json.Unmarshal(input, &before); err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(stripped, &after); err != nil {
				t.Fatal(err)
			}
			delete(before, backendField)
			if len(before) != len(after) {
				t.Fatalf("stock config fields changed: %s", stripped)
			}
			for key, value := range before {
				if string(value) != string(after[key]) {
					t.Fatalf("stock config field %s changed: %s", key, stripped)
				}
			}
		})
	}
}

func TestBackendConfigDefaultPreservesBytes(t *testing.T) {
	input := []byte(`{ "state-scheme": "firewood", "pruning-enabled": true, "state-sync-enabled": false }`)
	backend, stripped, err := backendConfig(input, constants.FujiID)
	if err != nil {
		t.Fatal(err)
	}
	if backend != "firewood" || string(stripped) != string(input) {
		t.Fatalf("default backend = %q, config = %s", backend, stripped)
	}
}

func TestBackendConfigRejectsUnsupportedModes(t *testing.T) {
	for _, fields := range []string{
		`"benchmark-state-backend":"unknown"`,
		`"benchmark-state-backend":null`,
		`"benchmark-state-backend":7`,
		`"pruning-enabled":false`,
		`"state-sync-enabled":true`,
		`"state-scheme":"hash"`,
	} {
		t.Run(fields, func(t *testing.T) {
			input := []byte(`{"state-scheme":"firewood","pruning-enabled":true,"state-sync-enabled":false,` + fields + `}`)
			if _, _, err := backendConfig(input, constants.FujiID); err == nil {
				t.Fatalf("accepted unsupported config: %s", input)
			}
		})
	}
}
