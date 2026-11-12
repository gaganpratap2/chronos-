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

var _ SnapshotInstallMarkerStorage = (*DiskStorage)(nil)

type snapshotInstallMarkerRecord struct {
	Pending bool
	Marker  SnapshotInstallMarker
}

func (s *DiskStorage) LoadSnapshotInstallMarker() (
	SnapshotInstallMarker,
	bool,
	error,
) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.file == nil {
		return SnapshotInstallMarker{}, false, os.ErrClosed
	}

	file, err := os.OpenFile(
		s.path+".snapshot-install",
		os.O_CREATE|os.O_RDWR,
		0600,
	)
	if err != nil {
		return SnapshotInstallMarker{}, false, err
	}
	defer file.Close()

	return readLatestSnapshotInstallMarker(file)
}

func (s *DiskStorage) SaveSnapshotInstallMarker(
	marker SnapshotInstallMarker,
) error {
	if err := validateSnapshotInstallMarker(marker); err != nil {
		return err
	}

	return s.appendSnapshotInstallMarker(
		snapshotInstallMarkerRecord{
			Pending: true,
			Marker:  marker,
		},
	)
}

func (s *DiskStorage) ClearSnapshotInstallMarker() error {
	return s.appendSnapshotInstallMarker(
		snapshotInstallMarkerRecord{},
	)
}

func (s *DiskStorage) appendSnapshotInstallMarker(
	record snapshotInstallMarkerRecord,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.file == nil {
		return os.ErrClosed
	}

	file, err := os.OpenFile(
		s.path+".snapshot-install",
		os.O_CREATE|os.O_RDWR,
		0600,
	)
	if err != nil {
		return err
	}
	defer file.Close()

	if _, _, err := readLatestSnapshotInstallMarker(file); err != nil {
		return err
	}

	data, err := json.Marshal(record)
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
		return errors.New("incomplete snapshot install marker write")
	}

	return file.Sync()
}

func readLatestSnapshotInstallMarker(
	file *os.File,
) (SnapshotInstallMarker, bool, error) {
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return SnapshotInstallMarker{}, false, err
	}

	reader := bufio.NewReader(file)
	var latest snapshotInstallMarkerRecord
	var validSize int64

	for {
		line, err := reader.ReadString('\n')

		if err == io.EOF {
			if len(line) > 0 {
				if err := file.Truncate(validSize); err != nil {
					return SnapshotInstallMarker{}, false, err
				}
				if err := file.Sync(); err != nil {
					return SnapshotInstallMarker{}, false, err
				}
			}
			break
		}
		if err != nil {
			return SnapshotInstallMarker{}, false, err
		}

		var record snapshotInstallMarkerRecord
		if err := json.Unmarshal(
			[]byte(strings.TrimSpace(line)),
			&record,
		); err != nil {
			return SnapshotInstallMarker{}, false, fmt.Errorf(
				"%w: cannot decode journal record: %v",
				ErrInvalidSnapshotInstallMarker,
				err,
			)
		}

		if record.Pending {
			if err := validateSnapshotInstallMarker(record.Marker); err != nil {
				return SnapshotInstallMarker{}, false, err
			}
		} else if record.Marker != (SnapshotInstallMarker{}) {
			return SnapshotInstallMarker{}, false,
				ErrInvalidSnapshotInstallMarker
		}

		latest = record
		validSize += int64(len(line))
	}

	return latest.Marker, latest.Pending, nil
}
