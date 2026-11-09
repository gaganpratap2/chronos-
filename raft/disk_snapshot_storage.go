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

	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return Snapshot{}, err
	}

	reader := bufio.NewReader(file)

	var latest Snapshot
	var validSize int64

	for {
		line, err := reader.ReadString('\n')

		if err == io.EOF {
			// Discard an incomplete final record.
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

func (s *DiskStorage) SaveSnapshot(snapshot Snapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.file == nil {
		return os.ErrClosed
	}

	if err := validateSnapshot(snapshot); err != nil {
		return err
	}

	data, err := json.Marshal(snapshot)
	if err != nil {
		return err
	}

	data = append(data, '\n')

	file, err := os.OpenFile(
		s.path+".snapshots",
		os.O_CREATE|os.O_RDWR,
		0600,
	)

	if err != nil {
		return err
	}

	if _, err := file.Seek(0, io.SeekEnd); err != nil {
		file.Close()
		return err
	}

	n, err := file.Write(data)
	if err != nil {
		file.Close()
		return err
	}

	if n != len(data) {
		file.Close()
		return errors.New("incomplete snapshot write")
	}

	if err := file.Sync(); err != nil {
		file.Close()
		return err
	}

	return file.Close()
}
