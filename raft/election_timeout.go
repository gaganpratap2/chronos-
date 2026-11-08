package raft

import (
	"errors"
	"math/rand"
	"time"
)

var (
	ErrInvalidElectionTimeout = errors.New(
		"invalid election timeout range",
	)
)

type ElectionConfig struct { // controls randomized election timing.
	MinTimeout time.Duration
	MaxTimeout time.Duration
}

// check timeout range

func (c ElectionConfig) Validate() error {
	if c.MinTimeout <= 0 ||
		c.MaxTimeout <= c.MinTimeout {
		return ErrInvalidElectionTimeout
	}

	return nil
}

func (c ElectionConfig) RandomTimeout() (time.Duration, error) {
	if err := c.Validate(); err != nil {
		return 0, err
	}

	difference := int64(c.MaxTimeout - c.MinTimeout)
	randomOffset := rand.Int63n(difference)

	return c.MinTimeout + time.Duration(randomOffset), nil
}
