package execution

import (
	"time"
)

const (
	MaxRecoveryAttempts = 5

	InitialRecoveryDelay = 30 * time.Second
	MaximumRecoveryDelay = 8 * time.Minute
)

func RecoveryDelay(attempt int) time.Duration {

	if attempt <= 0 {
		return InitialRecoveryDelay
	}

	delay := InitialRecoveryDelay

	for i := 1; i < attempt; i++ {
		delay *= 2

		if delay >= MaximumRecoveryDelay {
			return MaximumRecoveryDelay
		}
	}

	return delay
}
