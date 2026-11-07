package raft

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

var (
	ErrInvalidStoragePath = errors.New("raft storage path cannot be empty")
)

type DiskStorage struct {
	mu   sync.Mutex
	file *os.File
	path string
}

func NewDiskStorage(path string) (*DiskStorage, error) {

	if strings.TrimSpace(path) == "" {
		return nil, ErrInvalidStoragePath
	}

	// filepath.Dir finds the parent directory. os.MkdirAll creates it if necessary

	dir := filepath.Dir(path)

	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}

	file, err := os.OpenFile(
		path,
		// os.O_CREATE|os.O_RDWR|os.O_APPEND,
		os.O_CREATE|os.O_RDWR,
		0600,
	)
	if err != nil {
		return nil, err
	}

	return &DiskStorage{
		file: file,
		path: path,
	}, nil
}

func (s *DiskStorage) SaveHardState(state HardState) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.file == nil {
		return os.ErrClosed
	}

	if err := validateHardState(state); err != nil {
		return err
	}

	data, err := json.Marshal(state)
	if err != nil {
		return err
	}

	data = append(data, '\n')

	// Always write at the end of the journal.
	if _, err := s.file.Seek(0, io.SeekEnd); err != nil {
		return err
	}

	n, err := s.file.Write(data)
	if err != nil {
		return err
	}

	if n != len(data) {
		return errors.New("incomplete hard state write")
	}

	if err := s.file.Sync(); err != nil {
		return err
	}

	return nil
}

func (s *DiskStorage) LoadHardState() (HardState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.file == nil {
		return HardState{}, os.ErrClosed
	}

	if _, err := s.file.Seek(0, io.SeekStart); err != nil {
		return HardState{}, err
	}

	reader := bufio.NewReader(s.file)

	var latest HardState
	var validSize int64

	for {
		line, err := reader.ReadString('\n')

		if err == io.EOF {
			// Remove an incomplete final record.
			if len(line) > 0 {
				if truncateErr := s.file.Truncate(validSize); truncateErr != nil {
					return HardState{}, truncateErr
				}

				if syncErr := s.file.Sync(); syncErr != nil {
					return HardState{}, syncErr
				}
			}
			break
		}

		if err != nil {
			return HardState{}, err
		}

		var state HardState

		if err := json.Unmarshal([]byte(strings.TrimSpace(line)), &state); err != nil {
			return HardState{}, fmt.Errorf(
				"%w: cannot decode record: %v",
				ErrInvalidHardState,
				err,
			)
		}

		if err := validateHardState(state); err != nil {
			return HardState{}, err
		}

		latest = state
		validSize += int64(len(line))
	}

	if _, err := s.file.Seek(0, io.SeekEnd); err != nil {
		return HardState{}, err
	}

	return latest, nil
}

func (s *DiskStorage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.file == nil {
		return nil
	}

	err := s.file.Close()
	s.file = nil

	return err
}
