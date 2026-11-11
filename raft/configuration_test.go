package raft

import (
	"errors"
	"reflect"
	"testing"
)

func TestConfigurationValidateStable(t *testing.T) {
	config := ConfigurationState{
		OldVoters: []string{"A", "B", "C"},
	}

	if err := config.Validate(); err != nil {
		t.Fatalf("expected valid stable configuration, got %v", err)
	}

	if config.IsJoint() {
		t.Fatal("stable configuration was identified as joint")
	}
}

func TestConfigurationValidateJoint(t *testing.T) {
	config := ConfigurationState{
		OldVoters: []string{"A", "B", "C"},
		NewVoters: []string{"A", "B", "D"},
	}

	if err := config.Validate(); err != nil {
		t.Fatalf("expected valid joint configuration, got %v", err)
	}

	if !config.IsJoint() {
		t.Fatal("joint configuration was identified as stable")
	}
}

func TestConfigurationRejectsInvalidVoters(t *testing.T) {
	tests := []struct {
		name   string
		config ConfigurationState
	}{
		{
			name: "empty old voters",
			config: ConfigurationState{
				OldVoters: []string{},
			},
		},
		{
			name: "empty voter ID",
			config: ConfigurationState{
				OldVoters: []string{"A", ""},
			},
		},
		{
			name: "whitespace voter ID",
			config: ConfigurationState{
				OldVoters: []string{"A", "   "},
			},
		},
		{
			name: "padded voter ID",
			config: ConfigurationState{
				OldVoters: []string{"A", " B"},
			},
		},
		{
			name: "duplicate old voter",
			config: ConfigurationState{
				OldVoters: []string{"A", "B", "A"},
			},
		},
		{
			name: "duplicate new voter",
			config: ConfigurationState{
				OldVoters: []string{"A", "B", "C"},
				NewVoters: []string{"A", "D", "D"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()

			if !errors.Is(err, ErrInvalidConfiguration) {
				t.Fatalf(
					"expected ErrInvalidConfiguration, got %v",
					err,
				)
			}
		})
	}
}

func TestConfigurationCloneIsIndependent(t *testing.T) {
	original := ConfigurationState{
		OldVoters: []string{"A", "B", "C"},
		NewVoters: []string{"A", "B", "D"},
	}

	cloned := original.Clone()

	cloned.OldVoters[0] = "X"
	cloned.NewVoters[2] = "Y"

	if reflect.DeepEqual(original, cloned) {
		t.Fatal("changing clone unexpectedly changed original")
	}

	if original.OldVoters[0] != "A" {
		t.Fatal("original old voters were modified")
	}

	if original.NewVoters[2] != "D" {
		t.Fatal("original new voters were modified")
	}
}
