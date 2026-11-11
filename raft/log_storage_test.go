package raft

import (
	"reflect"
	"testing"
)

func TestMemoryStorageSaveAndLoadLog(t *testing.T) {
	storage := NewMemoryStorage()

	want := LogState{
		FirstIndex: 4,
		Entries: []LogEntry{
			{Term: 2, Command: []byte("task-4")},
			{Term: 3, Command: []byte("task-5")},
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

func TestMemoryStorageLogReturnsIndependentCopy(t *testing.T) {
	storage := NewMemoryStorage()

	original := LogState{
		FirstIndex: 1,
		Entries: []LogEntry{
			{Term: 1, Command: []byte("original")},
		},
	}

	if err := storage.SaveLog(original); err != nil {
		t.Fatal(err)
	}

	original.Entries[0].Command[0] = 'X'

	got, err := storage.LoadLog()
	if err != nil {
		t.Fatal(err)
	}

	if string(got.Entries[0].Command) != "original" {
		t.Fatalf("stored command was modified: %q", got.Entries[0].Command)
	}

	got.Entries[0].Command[0] = 'Y'

	again, err := storage.LoadLog()
	if err != nil {
		t.Fatal(err)
	}

	if string(again.Entries[0].Command) != "original" {
		t.Fatalf("loaded command modified stored data: %q", again.Entries[0].Command)
	}
}

func TestMemoryStorageRejectsInvalidLogState(t *testing.T) {
	tests := []struct {
		name  string
		state LogState
	}{
		{
			name: "zero first index",
			state: LogState{
				FirstIndex: 0,
			},
		},
		{
			name: "zero entry term",
			state: LogState{
				FirstIndex: 1,
				Entries: []LogEntry{
					{Term: 0, Command: []byte("invalid")},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := storageForInvalidLogTest().SaveLog(tt.state)

			if err != ErrInvalidLogState {
				t.Fatalf("SaveLog() error = %v, want %v", err, ErrInvalidLogState)
			}
		})
	}
}

func storageForInvalidLogTest() *MemoryStorage {
	return NewMemoryStorage()
}

func TestMemoryStorageLoadLogInitiallyEmpty(t *testing.T) {
	storage := NewMemoryStorage()

	got, err := storage.LoadLog()
	if err != nil {
		t.Fatal(err)
	}

	want := LogState{
		FirstIndex: 1,
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("LoadLog() = %+v, want %+v", got, want)
	}
}
