package raft

import (
	"errors"
	"strings"
)

var (
	ErrInvalidConfiguration = errors.New("invalid raft cluster configuration")
)

type ConfigurationState struct {
	OldVoters []string
	NewVoters []string
}

func (c ConfigurationState) IsJoint() bool {
	return len(c.NewVoters) > 0
}

func (c ConfigurationState) Validate() error {
	if err := validateVoters(c.OldVoters); err != nil {
		return err
	}

	if c.IsJoint() {
		if err := validateVoters(c.NewVoters); err != nil {
			return err
		}
	}

	return nil
}

func validateVoters(voters []string) error {
	if len(voters) == 0 {
		return ErrInvalidConfiguration
	}

	seen := make(map[string]struct{}, len(voters))

	for _, voter := range voters {
		if strings.TrimSpace(voter) == "" {
			return ErrInvalidConfiguration
		}

		if voter != strings.TrimSpace(voter) {
			return ErrInvalidConfiguration
		}

		if _, exists := seen[voter]; exists {
			return ErrInvalidConfiguration
		}

		seen[voter] = struct{}{}
	}

	return nil
}

func (c ConfigurationState) Clone() ConfigurationState {
	return ConfigurationState{
		OldVoters: append([]string(nil), c.OldVoters...),
		NewVoters: append([]string(nil), c.NewVoters...),
	}
}

// HasQuorum reports whether the supplied voters satisfy
// the active configuration's quorum requirements.
func (c ConfigurationState) HasQuorum(votes []string) bool {
	if c.Validate() != nil {
		return false
	}

	voteSet := make(map[string]struct{}, len(votes))
	for _, vote := range votes {
		voteSet[vote] = struct{}{}
	}

	if !hasVoterMajority(c.OldVoters, voteSet) {
		return false
	}

	if c.IsJoint() && !hasVoterMajority(c.NewVoters, voteSet) {
		return false
	}

	return true
}

// hasVoterMajority checks whether a majority of one voter set
// appears in the supplied votes.
func hasVoterMajority(
	voters []string,
	votes map[string]struct{},
) bool {
	count := 0

	for _, voter := range voters {
		if _, exists := votes[voter]; exists {
			count++
		}
	}

	return count >= len(voters)/2+1
}

// configurationPeerIDs returns every configured voter except the local node.
// During joint consensus, it returns the union of old and new voters.
func configurationPeerIDs(
	localID string,
	configuration ConfigurationState,
) []string {
	peers := make([]string, 0)

	seen := make(map[string]struct{})

	for _, voter := range configuration.OldVoters {
		if voter == localID {
			continue
		}

		if _, exists := seen[voter]; exists {
			continue
		}

		seen[voter] = struct{}{}
		peers = append(peers, voter)
	}

	for _, voter := range configuration.NewVoters {
		if voter == localID {
			continue
		}

		if _, exists := seen[voter]; exists {
			continue
		}

		seen[voter] = struct{}{}
		peers = append(peers, voter)
	}

	return peers
}

// IsVoter reports whether an ID belongs to either active voter set.
// During joint consensus, both old and new voters are eligible.
func (c ConfigurationState) IsVoter(id string) bool {
	if id == "" || c.Validate() != nil {
		return false
	}

	for _, voter := range c.OldVoters {
		if voter == id {
			return true
		}
	}

	for _, voter := range c.NewVoters {
		if voter == id {
			return true
		}
	}

	return false
}
