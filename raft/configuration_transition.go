package raft

import "errors"

var (
	ErrInvalidConfigurationTransition = errors.New(
		"invalid raft configuration transition",
	)
)

// ValidateConfigurationTransition checks whether next is a legal
// next configuration after current.
func ValidateConfigurationTransition(
	current ConfigurationState,
	next ConfigurationState,
) error {
	if err := current.Validate(); err != nil {
		return ErrInvalidConfigurationTransition
	}

	if err := next.Validate(); err != nil {
		return ErrInvalidConfigurationTransition
	}

	// A stable configuration may only enter joint consensus.
	if !current.IsJoint() {
		if !next.IsJoint() {
			return ErrInvalidConfigurationTransition
		}

		if !equalVoterSets(current.OldVoters, next.OldVoters) {
			return ErrInvalidConfigurationTransition
		}

		return nil
	}

	// A joint configuration may only become its intended
	// stable new configuration.
	if next.IsJoint() {
		return ErrInvalidConfigurationTransition
	}

	if !equalVoterSets(current.NewVoters, next.OldVoters) {
		return ErrInvalidConfigurationTransition
	}

	return nil
}

func equalVoterSets(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}

	set := make(map[string]struct{}, len(a))

	for _, voter := range a {
		set[voter] = struct{}{}
	}

	for _, voter := range b {
		if _, exists := set[voter]; !exists {
			return false
		}
	}

	return true
}
