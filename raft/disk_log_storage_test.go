package raft

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestDiskStorageSaveAndLoadLog(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft-state.jsonl")

	storage, err := NewDiskStorage(path)
	if err != nil {
		t.Fatal(err)
	}
	defer storage.Close()

	want := LogState{
		FirstIndex: 1,
		Entries: []LogEntry{
			{Term: 1, Command: []byte("first")},
			{Term: 2, Command: []byte("second")},
		},
	}

	if err := storage.SaveLog(want); err != nil {
		t.Fatalf("SaveLog() error = %v", err)
	}

	got, err := storage.LoadLog()
	if err != nil {
		t.Fatalf("LoadLog() error = %v", err)
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("LoadLog() = %+v, want %+v", got, want)
	}
}

func TestDiskStorageLogSurvivesReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft-state.jsonl")

	storage, err := NewDiskStorage(path)
	if err != nil {
		t.Fatal(err)
	}

	want := LogState{
		FirstIndex: 4,
		Entries: []LogEntry{
			{Term: 2, Command: []byte("command")},
		},
	}

	if err := storage.SaveLog(want); err != nil {
		storage.Close()
		t.Fatal(err)
	}

	if err := storage.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := NewDiskStorage(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	got, err := reopened.LoadLog()
	if err != nil {
		t.Fatalf("LoadLog() after reopen error = %v", err)
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("recovered log = %+v, want %+v", got, want)
	}
}

func TestDiskStorageTruncatesIncompleteFinalLogRecord(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft-state.jsonl")

	storage, err := NewDiskStorage(path)
	if err != nil {
		t.Fatal(err)
	}

	want := LogState{
		FirstIndex: 1,
		Entries: []LogEntry{
			{Term: 1, Command: []byte("durable")},
		},
	}

	if err := storage.SaveLog(want); err != nil {
		storage.Close()
		t.Fatal(err)
	}

	if err := storage.Close(); err != nil {
		t.Fatal(err)
	}

	logPath := path + ".log"

	file, err := os.OpenFile(logPath, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := file.WriteString(`{"FirstIndex":2`); err != nil {
		file.Close()
		t.Fatal(err)
	}

	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := NewDiskStorage(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	got, err := reopened.LoadLog()
	if err != nil {
		t.Fatalf("LoadLog() error = %v", err)
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("recovered log = %+v, want %+v", got, want)
	}
}

func TestDiskStorageRejectsCorruptCompleteLogRecord(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft-state.jsonl")

	storage, err := NewDiskStorage(path)
	if err != nil {
		t.Fatal(err)
	}
	defer storage.Close()

	logPath := path + ".log"

	if err := os.WriteFile(
		logPath,
		[]byte("{invalid-json}\n"),
		0600,
	); err != nil {
		t.Fatal(err)
	}

	_, err = storage.LoadLog()
	if err == nil {
		t.Fatal("LoadLog() expected an error for corrupt record")
	}

	if !errors.Is(err, ErrInvalidLogState) {
		t.Fatalf("LoadLog() error = %v, want ErrInvalidLogState", err)
	}
}

func TestDiskStorageLoadLogReturnsIndependentCopy(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft-state.jsonl")

	storage, err := NewDiskStorage(path)
	if err != nil {
		t.Fatal(err)
	}
	defer storage.Close()

	want := LogState{
		FirstIndex: 1,
		Entries: []LogEntry{
			{Term: 1, Command: []byte("original")},
		},
	}

	if err := storage.SaveLog(want); err != nil {
		t.Fatal(err)
	}

	first, err := storage.LoadLog()
	if err != nil {
		t.Fatal(err)
	}

	first.Entries[0].Command[0] = 'X'

	second, err := storage.LoadLog()
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(second, want) {
		t.Fatalf("stored log changed after caller mutation: got %+v, want %+v", second, want)
	}
}

func TestDiskStorageSaveLogRecoversIncompleteTailBeforeAppend(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft-state.jsonl")

	storage, err := NewDiskStorage(path)
	if err != nil {
		t.Fatal(err)
	}

	first := LogState{
		FirstIndex: 1,
		Entries: []LogEntry{
			{Term: 1, Command: []byte("first")},
		},
	}

	if err := storage.SaveLog(first); err != nil {
		t.Fatalf("save first log: %v", err)
	}

	if err := storage.Close(); err != nil {
		t.Fatal(err)
	}

	file, err := os.OpenFile(
		path+".log",
		os.O_APPEND|os.O_WRONLY,
		0600,
	)
	if err != nil {
		t.Fatal(err)
	}

	_, writeErr := file.WriteString(`{"FirstIndex":2`)
	closeErr := file.Close()

	if writeErr != nil {
		t.Fatalf("write incomplete log record: %v", writeErr)
	}
	if closeErr != nil {
		t.Fatal(closeErr)
	}

	reopened, err := NewDiskStorage(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	// Do not call LoadLog before saving.
	second := LogState{
		FirstIndex: 1,
		Entries: []LogEntry{
			{Term: 1, Command: []byte("first")},
			{Term: 2, Command: []byte("second")},
		},
	}

	if err := reopened.SaveLog(second); err != nil {
		t.Fatalf("save log after incomplete tail: %v", err)
	}

	got, err := reopened.LoadLog()
	if err != nil {
		t.Fatalf("load log after recovery: %v", err)
	}

	if !reflect.DeepEqual(got, second) {
		t.Fatalf("recovered log = %+v, want %+v", got, second)
	}
}
