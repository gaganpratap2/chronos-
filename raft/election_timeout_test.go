package raft

import (
	"errors"
	"testing"
	"time"
)

func TestElectionConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		config  ElectionConfig
		wantErr bool
	}{
		{
			name: "valid range",
			config: ElectionConfig{
				MinTimeout: 150 * time.Millisecond,
				MaxTimeout: 300 * time.Millisecond,
			},
		},
		{
			name: "zero minimum",
			config: ElectionConfig{
				MinTimeout: 0,
				MaxTimeout: 300 * time.Millisecond,
			},
			wantErr: true,
		},
		{
			name: "negative minimum",
			config: ElectionConfig{
				MinTimeout: -100 * time.Millisecond,
				MaxTimeout: 300 * time.Millisecond,
			},
			wantErr: true,
		},
		{
			name: "equal minimum and maximum",
			config: ElectionConfig{
				MinTimeout: 200 * time.Millisecond,
				MaxTimeout: 200 * time.Millisecond,
			},
			wantErr: true,
		},
		{
			name: "maximum below minimum",
			config: ElectionConfig{
				MinTimeout: 300 * time.Millisecond,
				MaxTimeout: 200 * time.Millisecond,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()

			if tt.wantErr && !errors.Is(err, ErrInvalidElectionTimeout) {
				t.Fatalf("expected ErrInvalidElectionTimeout, got %v", err)
			}

			if !tt.wantErr && err != nil {
				t.Fatalf("expected no error, got %v", err)
			}
		})
	}
}

func TestElectionConfigRandomTimeout(t *testing.T) {
	config := ElectionConfig{
		MinTimeout: 150 * time.Millisecond,
		MaxTimeout: 300 * time.Millisecond,
	}

	for i := 0; i < 1000; i++ {
		timeout, err := config.RandomTimeout()
		if err != nil {
			t.Fatalf("RandomTimeout returned error: %v", err)
		}

		if timeout < config.MinTimeout {
			t.Fatalf("timeout %v is below minimum %v",
				timeout, config.MinTimeout)
		}

		if timeout >= config.MaxTimeout {
			t.Fatalf("timeout %v is not below maximum %v",
				timeout, config.MaxTimeout)
		}
	}
}

func TestElectionConfigRandomTimeoutRejectsInvalidConfig(t *testing.T) {
	config := ElectionConfig{
		MinTimeout: 300 * time.Millisecond,
		MaxTimeout: 200 * time.Millisecond,
	}

	timeout, err := config.RandomTimeout()

	if !errors.Is(err, ErrInvalidElectionTimeout) {
		t.Fatalf("expected ErrInvalidElectionTimeout, got %v", err)
	}

	if timeout != 0 {
		t.Fatalf("expected zero timeout, got %v", timeout)
	}
}
