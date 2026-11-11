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

// Compile-time check that DiskStorage supports snapshot persistence.
var _ SnapshotStorage = (*DiskStorage)(nil)

func (s *DiskStorage) LoadSnapshot() (Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.file == nil {
		return Snapshot{}, os.ErrClosed
	}

	file, err := os.OpenFile(
		s.path+".snapshots",
		os.O_CREATE|os.O_RDWR,
		0600,
	)
	if err != nil {
		return Snapshot{}, err
	}
	defer file.Close()

	return readLatestSnapshot(file)
}

// readLatestSnapshot loads the latest valid snapshot record.
//
// If the final record is incomplete, it truncates that record.
// A complete but corrupt record is rejected.
func readLatestSnapshot(file *os.File) (Snapshot, error) {
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return Snapshot{}, err
	}

	reader := bufio.NewReader(file)

	var latest Snapshot
	var validSize int64

	for {
		line, err := reader.ReadString('\n')

		if err == io.EOF {
			if len(line) > 0 {
				if truncateErr := file.Truncate(validSize); truncateErr != nil {
					return Snapshot{}, truncateErr
				}

				if syncErr := file.Sync(); syncErr != nil {
					return Snapshot{}, syncErr
				}
			}
			break
		}

		if err != nil {
			return Snapshot{}, err
		}

		var snapshot Snapshot

		if decodeErr := json.Unmarshal(
			[]byte(strings.TrimSpace(line)),
			&snapshot,
		); decodeErr != nil {
			return Snapshot{}, fmt.Errorf(
				"%w: cannot decode snapshot record: %v",
				ErrInvalidSnapshot,
				decodeErr,
			)
		}

		if validateErr := validateSnapshot(snapshot); validateErr != nil {
			return Snapshot{}, validateErr
		}

		latest = cloneSnapshot(snapshot)
		validSize += int64(len(line))
	}

	return cloneSnapshot(latest), nil
}

func (s *DiskStorage) SaveSnapshot(snapshot Snapshot) (resultErr error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.file == nil {
		return os.ErrClosed
	}

	if err := validateSnapshot(snapshot); err != nil {
		return err
	}

	file, err := os.OpenFile(
		s.path+".snapshots",
		os.O_CREATE|os.O_RDWR,
		0600,
	)
	if err != nil {
		return err
	}

	defer func() {
		if closeErr := file.Close(); resultErr == nil {
			resultErr = closeErr
		}
	}()

	// Recover the journal before appending.
	// This removes an incomplete final record and rejects
	// any complete corrupt record.
	if _, err := readLatestSnapshot(file); err != nil {
		return fmt.Errorf("recover snapshot journal before append: %w", err)
	}

	data, err := json.Marshal(snapshot)
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
		return errors.New("incomplete snapshot write")
	}

	if err := file.Sync(); err != nil {
		return err
	}

	return nil
}
