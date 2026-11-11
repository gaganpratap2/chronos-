package raft

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
)

var (
	ErrConfigurationNotFound = errors.New(
		"raft configuration has not been persisted",
	)
	ErrInvalidConfigurationRecord = errors.New(
		"invalid persisted raft configuration",
	)
)

var _ ConfigurationStorage = (*DiskStorage)(nil)
var _ IndexedConfigurationStorage = (*DiskStorage)(nil)

type persistedConfigurationRecord struct {
	Index         uint64             `json:"index"`
	Configuration ConfigurationState `json:"configuration"`
}

// readConfigurationJournal reads both legacy and indexed records.
// An incomplete final line is truncated. Complete corrupt records are rejected.
func readConfigurationJournal(
	file *os.File,
) (ConfigurationState, []ConfigurationRecord, error) {
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return ConfigurationState{}, nil, err
	}

	reader := bufio.NewReader(file)

	var latest ConfigurationState
	var records []ConfigurationRecord
	var validSize int64
	var found bool

	for {
		line, err := reader.ReadString('\n')

		if err == io.EOF {
			if len(line) > 0 {
				if truncateErr := file.Truncate(validSize); truncateErr != nil {
					return ConfigurationState{}, nil, truncateErr
				}
				if syncErr := file.Sync(); syncErr != nil {
					return ConfigurationState{}, nil, syncErr
				}
			}
			break
		}

		if err != nil {
			return ConfigurationState{}, nil, err
		}

		trimmed := strings.TrimSpace(line)

		var raw map[string]json.RawMessage
		if decodeErr := json.Unmarshal([]byte(trimmed), &raw); decodeErr != nil {
			return ConfigurationState{}, nil, fmt.Errorf(
				"%w: cannot decode configuration record: %v",
				ErrInvalidConfigurationRecord,
				decodeErr,
			)
		}

		var configuration ConfigurationState

		if _, indexed := raw["configuration"]; indexed {
			var record persistedConfigurationRecord

			if decodeErr := json.Unmarshal([]byte(trimmed), &record); decodeErr != nil {
				return ConfigurationState{}, nil, fmt.Errorf(
					"%w: cannot decode indexed record: %v",
					ErrInvalidConfigurationRecord,
					decodeErr,
				)
			}

			if record.Index == 0 {
				return ConfigurationState{}, nil, ErrInvalidConfigurationIndex
			}

			if validateErr := record.Configuration.Validate(); validateErr != nil {
				return ConfigurationState{}, nil, fmt.Errorf(
					"%w: %v",
					ErrInvalidConfigurationRecord,
					validateErr,
				)
			}

			if len(records) > 0 {
				last := records[len(records)-1]

				if record.Index <= last.Index {
					return ConfigurationState{}, nil, ErrInvalidConfigurationIndex
				}
			}

			configuration = record.Configuration.Clone()

			records = append(records, ConfigurationRecord{
				Index:         record.Index,
				Configuration: configuration.Clone(),
			})
		} else {
			if decodeErr := json.Unmarshal([]byte(trimmed), &configuration); decodeErr != nil {
				return ConfigurationState{}, nil, fmt.Errorf(
					"%w: cannot decode legacy record: %v",
					ErrInvalidConfigurationRecord,
					decodeErr,
				)
			}

			if validateErr := configuration.Validate(); validateErr != nil {
				return ConfigurationState{}, nil, fmt.Errorf(
					"%w: %v",
					ErrInvalidConfigurationRecord,
					validateErr,
				)
			}
		}

		latest = configuration.Clone()
		validSize += int64(len(line))
		found = true
	}

	if !found {
		return ConfigurationState{}, records, ErrConfigurationNotFound
	}

	return latest, records, nil
}

// LoadConfiguration returns the latest persisted configuration.
func (s *DiskStorage) LoadConfiguration() (ConfigurationState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.file == nil {
		return ConfigurationState{}, os.ErrClosed
	}

	file, err := os.OpenFile(
		s.path+".configuration",
		os.O_CREATE|os.O_RDWR,
		0600,
	)
	if err != nil {
		return ConfigurationState{}, err
	}
	defer file.Close()

	configuration, _, err := readConfigurationJournal(file)
	return configuration, err
}

// SaveConfiguration preserves the legacy unindexed persistence API.
func (s *DiskStorage) SaveConfiguration(
	configuration ConfigurationState,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.file == nil {
		return os.ErrClosed
	}

	if err := configuration.Validate(); err != nil {
		return err
	}

	file, err := os.OpenFile(
		s.path+".configuration",
		os.O_CREATE|os.O_RDWR,
		0600,
	)
	if err != nil {
		return err
	}
	defer file.Close()

	if _, _, err := readConfigurationJournal(file); err != nil &&
		!errors.Is(err, ErrConfigurationNotFound) {
		return err
	}

	data, err := json.Marshal(configuration)
	if err != nil {
		return err
	}

	return appendConfigurationRecord(file, data)
}

// LoadConfigurationRecords returns all indexed configuration records.
func (s *DiskStorage) LoadConfigurationRecords() (
	[]ConfigurationRecord,
	error,
) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.file == nil {
		return nil, os.ErrClosed
	}

	file, err := os.OpenFile(
		s.path+".configuration",
		os.O_CREATE|os.O_RDWR,
		0600,
	)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	_, records, err := readConfigurationJournal(file)
	if errors.Is(err, ErrConfigurationNotFound) {
		return []ConfigurationRecord{}, nil
	}
	if err != nil {
		return nil, err
	}

	return records, nil
}

// SaveConfigurationAt durably appends an indexed configuration record.
func (s *DiskStorage) SaveConfigurationAt(
	index uint64,
	configuration ConfigurationState,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.file == nil {
		return os.ErrClosed
	}

	if index == 0 {
		return ErrInvalidConfigurationIndex
	}

	if err := configuration.Validate(); err != nil {
		return err
	}

	file, err := os.OpenFile(
		s.path+".configuration",
		os.O_CREATE|os.O_RDWR,
		0600,
	)
	if err != nil {
		return err
	}
	defer file.Close()

	_, records, recoverErr := readConfigurationJournal(file)
	if recoverErr != nil &&
		!errors.Is(recoverErr, ErrConfigurationNotFound) {
		return fmt.Errorf("recover configuration journal: %w", recoverErr)
	}

	if len(records) > 0 {
		last := records[len(records)-1]

		if index == last.Index {
			if equalVoterSets(last.Configuration.OldVoters, configuration.OldVoters) &&
				equalVoterSets(last.Configuration.NewVoters, configuration.NewVoters) {
				return nil
			}
			return ErrConfigurationIndexConflict
		}

		if index < last.Index {
			return ErrInvalidConfigurationIndex
		}
	}

	record := persistedConfigurationRecord{
		Index:         index,
		Configuration: configuration.Clone(),
	}

	data, err := json.Marshal(record)
	if err != nil {
		return err
	}

	return appendConfigurationRecord(file, data)
}

func appendConfigurationRecord(file *os.File, data []byte) error {
	data = append(data, '\n')

	if _, err := file.Seek(0, io.SeekEnd); err != nil {
		return err
	}

	n, err := file.Write(data)
	if err != nil {
		return err
	}

	if n != len(data) {
		return errors.New("incomplete configuration write")
	}

	return file.Sync()
}
