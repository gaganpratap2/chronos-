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

var _ LogStorage = (*DiskStorage)(nil) // Compile-time check that DiskStorage supports durable log persistence

func (s *DiskStorage) LoadLog() (LogState, error) { // // LoadLog returns the latest valid retained-log record.
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.file == nil {
		return LogState{}, os.ErrClosed
	}

	file, err := os.OpenFile(
		s.path+".log",
		os.O_CREATE|os.O_RDWR,
		0600,
	)
	if err != nil {
		return LogState{}, err
	}
	defer file.Close()

	return readLatestLogState(file)
}

func (s *DiskStorage) SaveLog(state LogState) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.file == nil {
		return os.ErrClosed
	}

	if err := validateLogState(state); err != nil {
		return err
	}

	file, err := os.OpenFile(
		s.path+".log",
		os.O_CREATE|os.O_RDWR,
		0600,
	)
	if err != nil {
		return err
	}
	defer file.Close()

	if _, err := readLatestLogState(file); err != nil {
		return err
	}

	data, err := json.Marshal(cloneLogState(state))
	if err != nil {
		return err
	}

	data = append(data, '\n')

	if _, err := file.Seek(0, io.SeekEnd); err != nil {
		return err
	}

	n, err := file.Write(data)
	if err != nil {
		return err
	}

	if n != len(data) {
		return errors.New("incomplete log write")
	}

	if err := file.Sync(); err != nil {
		return err
	}

	return nil
}

func readLatestLogState(file *os.File) (LogState, error) {
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return LogState{}, err
	}

	reader := bufio.NewReader(file)

	latest := LogState{
		FirstIndex: 1,
	}

	var validSize int64

	for {
		line, err := reader.ReadString('\n')

		if err == io.EOF {
			if len(line) > 0 {
				if truncateErr := file.Truncate(validSize); truncateErr != nil {
					return LogState{}, truncateErr
				}

				if syncErr := file.Sync(); syncErr != nil {
					return LogState{}, syncErr
				}
			}
			break
		}

		if err != nil {
			return LogState{}, err
		}

		var state LogState

		if decodeErr := json.Unmarshal(
			[]byte(strings.TrimSpace(line)),
			&state,
		); decodeErr != nil {
			return LogState{}, fmt.Errorf(
				"%w: cannot decode log record: %v",
				ErrInvalidLogState,
				decodeErr,
			)
		}

		if validateErr := validateLogState(state); validateErr != nil {
			return LogState{}, validateErr
		}

		latest = cloneLogState(state)
		validSize += int64(len(line))
	}

	return cloneLogState(latest), nil
}
