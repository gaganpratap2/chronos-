package raft

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestMemoryStorageSaveAndLoad(t *testing.T) {
	storage := NewMemoryStorage()

	want := HardState{
		CurrentTerm: 5,
		VotedFor:    "node-B",
	}

	if err := storage.SaveHardState(want); err != nil {
		t.Fatal(err)
	}

	got, err := storage.LoadHardState()
	if err != nil {
		t.Fatal(err)
	}

	if got != want {
		t.Fatalf("LoadHardState() = %+v, want %+v", got, want)
	}
}

func TestNewNodeRestoresHardState(t *testing.T) {
	storage := NewMemoryStorage()

	want := HardState{
		CurrentTerm: 7,
		VotedFor:    "node-C",
	}

	if err := storage.SaveHardState(want); err != nil {
		t.Fatal(err)
	}

	node, err := NewNodeWithStorage(
		"node-A",
		[]string{"node-B", "node-C"},
		storage,
	)
	if err != nil {
		t.Fatal(err)
	}

	if node.CurrentTerm() != 7 {
		t.Errorf("CurrentTerm() = %d, want 7", node.CurrentTerm())
	}

	if node.VotedFor() != "node-C" {
		t.Errorf("VotedFor() = %q, want node-C", node.VotedFor())
	}

	if node.State() != StateFollower {
		t.Errorf("State() = %v, want follower", node.State())
	}
}

func TestNewNodeRejectsNilStorage(t *testing.T) {
	_, err := NewNodeWithStorage("node-A", nil, nil)

	if !errors.Is(err, ErrInvalidStorage) {
		t.Errorf("error = %v, want ErrInvalidStorage", err)
	}
}

func TestNodeRestoresDiskHardStateAfterRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft", "hard-state.log")

	// Start the first instance of the node.
	storage1, err := NewDiskStorage(path)
	if err != nil {
		t.Fatalf("create first storage: %v", err)
	}

	node1, err := NewNodeWithStorage(
		"node-A",
		[]string{"node-B", "node-C"},
		storage1,
	)
	if err != nil {
		t.Fatalf("create first node: %v", err)
	}

	if err := node1.StartElection(); err != nil {
		t.Fatalf("start election: %v", err)
	}

	if node1.CurrentTerm() != 1 {
		t.Fatalf("expected term 1, got %d", node1.CurrentTerm())
	}

	if err := storage1.Close(); err != nil {
		t.Fatalf("close first storage: %v", err)
	}

	// Simulate a process restart using the same journal file.
	storage2, err := NewDiskStorage(path)
	if err != nil {
		t.Fatalf("create second storage: %v", err)
	}
	defer storage2.Close()

	node2, err := NewNodeWithStorage(
		"node-A",
		[]string{"node-B", "node-C"},
		storage2,
	)
	if err != nil {
		t.Fatalf("restore second node: %v", err)
	}

	if node2.CurrentTerm() != 1 {
		t.Fatalf("expected restored term 1, got %d", node2.CurrentTerm())
	}

	if node2.VotedFor() != "node-A" {
		t.Fatalf("expected restored vote for node-A, got %q", node2.VotedFor())
	}

	if node2.State() != StateFollower {
		t.Fatalf("expected restarted node to be follower, got %s", node2.State())
	}
}

func TestMemoryStorageSaveAndLoadConfiguration(t *testing.T) {
	storage := NewMemoryStorage()

	want := ConfigurationState{
		OldVoters: []string{"node-A", "node-B", "node-C"},
	}

	if err := storage.SaveConfiguration(want); err != nil {
		t.Fatalf("SaveConfiguration(): %v", err)
	}

	got, err := storage.LoadConfiguration()
	if err != nil {
		t.Fatalf("LoadConfiguration(): %v", err)
	}

	if len(got.OldVoters) != len(want.OldVoters) {
		t.Fatalf("got voters %v, want %v", got.OldVoters, want.OldVoters)
	}

	for i := range want.OldVoters {
		if got.OldVoters[i] != want.OldVoters[i] {
			t.Fatalf("got voters %v, want %v", got.OldVoters, want.OldVoters)
		}
	}
}

func TestMemoryStorageConfigurationUsesIndependentCopies(t *testing.T) {
	storage := NewMemoryStorage()

	configuration := ConfigurationState{
		OldVoters: []string{"node-A", "node-B"},
	}

	if err := storage.SaveConfiguration(configuration); err != nil {
		t.Fatal(err)
	}

	// Mutating the original must not change stored configuration.
	configuration.OldVoters[0] = "node-X"

	got, err := storage.LoadConfiguration()
	if err != nil {
		t.Fatal(err)
	}

	if got.OldVoters[0] != "node-A" {
		t.Fatalf("stored configuration changed through input: %v", got)
	}

	// Mutating the returned value must not change stored configuration.
	got.OldVoters[0] = "node-Y"

	again, err := storage.LoadConfiguration()
	if err != nil {
		t.Fatal(err)
	}

	if again.OldVoters[0] != "node-A" {
		t.Fatalf("stored configuration changed through output: %v", again)
	}
}

func TestMemoryStorageRejectsInvalidConfiguration(t *testing.T) {
	storage := NewMemoryStorage()

	invalid := ConfigurationState{
		OldVoters: []string{"node-A", "node-A"},
	}

	if err := storage.SaveConfiguration(invalid); err == nil {
		t.Fatal("expected invalid configuration to be rejected")
	}
}

func TestDiskStorageConfigurationSurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft", "hard-state.log")

	first, err := NewDiskStorage(path)
	if err != nil {
		t.Fatal(err)
	}

	want := ConfigurationState{
		OldVoters: []string{"node-A", "node-B", "node-C"},
	}

	if err := first.SaveConfiguration(want); err != nil {
		t.Fatalf("SaveConfiguration(): %v", err)
	}

	// Save a newer joint configuration as well.
	joint := ConfigurationState{
		OldVoters: []string{"node-A", "node-B", "node-C"},
		NewVoters: []string{"node-A", "node-B", "node-D"},
	}

	if err := first.SaveConfiguration(joint); err != nil {
		t.Fatalf("SaveConfiguration(joint): %v", err)
	}

	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	second, err := NewDiskStorage(path)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()

	got, err := second.LoadConfiguration()
	if err != nil {
		t.Fatalf("LoadConfiguration(): %v", err)
	}

	if len(got.OldVoters) != len(joint.OldVoters) ||
		len(got.NewVoters) != len(joint.NewVoters) {
		t.Fatalf("got %+v, want %+v", got, joint)
	}

	for i := range joint.OldVoters {
		if got.OldVoters[i] != joint.OldVoters[i] {
			t.Fatalf("old voters = %v, want %v", got.OldVoters, joint.OldVoters)
		}
	}

	for i := range joint.NewVoters {
		if got.NewVoters[i] != joint.NewVoters[i] {
			t.Fatalf("new voters = %v, want %v", got.NewVoters, joint.NewVoters)
		}
	}
}

func TestDiskStorageConfigurationRejectsCorruptRecord(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft", "hard-state.log")

	storage, err := NewDiskStorage(path)
	if err != nil {
		t.Fatal(err)
	}
	defer storage.Close()

	if err := storage.SaveConfiguration(ConfigurationState{
		OldVoters: []string{"node-A", "node-B"},
	}); err != nil {
		t.Fatal(err)
	}

	file, err := os.OpenFile(
		path+".configuration",
		os.O_APPEND|os.O_WRONLY,
		0600,
	)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := file.WriteString("{invalid-json}\n"); err != nil {
		file.Close()
		t.Fatal(err)
	}

	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	_, err = storage.LoadConfiguration()
	if !errors.Is(err, ErrInvalidConfigurationRecord) {
		t.Fatalf("LoadConfiguration() error = %v, want ErrInvalidConfigurationRecord", err)
	}
}

func TestDiskStorageConfigurationRejectsInvalidConfiguration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft", "hard-state.log")

	storage, err := NewDiskStorage(path)
	if err != nil {
		t.Fatal(err)
	}
	defer storage.Close()

	err = storage.SaveConfiguration(ConfigurationState{
		OldVoters: []string{"node-A", "node-A"},
	})

	if err == nil {
		t.Fatal("expected invalid configuration to be rejected")
	}
}

func TestDiskStorageConfigurationReportsMissingConfiguration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft", "hard-state.log")

	storage, err := NewDiskStorage(path)
	if err != nil {
		t.Fatal(err)
	}
	defer storage.Close()

	_, err = storage.LoadConfiguration()

	if !errors.Is(err, ErrConfigurationNotFound) {
		t.Fatalf("LoadConfiguration() error = %v, want ErrConfigurationNotFound", err)
	}
}

func TestMemoryStorageSaveAndLoadConfigurationRecords(t *testing.T) {
	storage := NewMemoryStorage()

	first := ConfigurationState{
		OldVoters: []string{"node-A", "node-B", "node-C"},
	}

	second := ConfigurationState{
		OldVoters: []string{"node-A", "node-B", "node-C"},
		NewVoters: []string{"node-A", "node-B", "node-D"},
	}

	if err := storage.SaveConfigurationAt(5, first); err != nil {
		t.Fatalf("save first configuration: %v", err)
	}

	if err := storage.SaveConfigurationAt(8, second); err != nil {
		t.Fatalf("save second configuration: %v", err)
	}

	records, err := storage.LoadConfigurationRecords()
	if err != nil {
		t.Fatalf("load configuration records: %v", err)
	}

	if len(records) != 2 {
		t.Fatalf("got %d records, want 2", len(records))
	}

	if records[0].Index != 5 || records[1].Index != 8 {
		t.Fatalf("record indexes = %d, %d; want 5, 8",
			records[0].Index, records[1].Index)
	}

	if !reflect.DeepEqual(records[0].Configuration, first) {
		t.Fatalf("first configuration = %+v, want %+v",
			records[0].Configuration, first)
	}

	if !reflect.DeepEqual(records[1].Configuration, second) {
		t.Fatalf("second configuration = %+v, want %+v",
			records[1].Configuration, second)
	}
}

func TestMemoryStorageConfigurationRecordIsIdempotent(t *testing.T) {
	storage := NewMemoryStorage()

	configuration := ConfigurationState{
		OldVoters: []string{"node-A", "node-B"},
	}

	if err := storage.SaveConfigurationAt(5, configuration); err != nil {
		t.Fatal(err)
	}

	if err := storage.SaveConfigurationAt(5, configuration); err != nil {
		t.Fatalf("repeated identical write: %v", err)
	}

	records, err := storage.LoadConfigurationRecords()
	if err != nil {
		t.Fatal(err)
	}

	if len(records) != 1 {
		t.Fatalf("got %d records, want 1", len(records))
	}
}

func TestMemoryStorageRejectsConfigurationIndexConflict(t *testing.T) {
	storage := NewMemoryStorage()

	first := ConfigurationState{
		OldVoters: []string{"node-A", "node-B"},
	}

	second := ConfigurationState{
		OldVoters: []string{"node-A", "node-C"},
	}

	if err := storage.SaveConfigurationAt(5, first); err != nil {
		t.Fatal(err)
	}

	err := storage.SaveConfigurationAt(5, second)

	if !errors.Is(err, ErrConfigurationIndexConflict) {
		t.Fatalf("error = %v, want ErrConfigurationIndexConflict", err)
	}
}

func TestMemoryStorageRejectsInvalidConfigurationIndexes(t *testing.T) {
	storage := NewMemoryStorage()

	configuration := ConfigurationState{
		OldVoters: []string{"node-A"},
	}

	if err := storage.SaveConfigurationAt(0, configuration); !errors.Is(
		err, ErrInvalidConfigurationIndex,
	) {
		t.Fatalf("index zero error = %v, want ErrInvalidConfigurationIndex", err)
	}

	if err := storage.SaveConfigurationAt(5, configuration); err != nil {
		t.Fatal(err)
	}

	if err := storage.SaveConfigurationAt(4, configuration); !errors.Is(
		err, ErrInvalidConfigurationIndex,
	) {
		t.Fatalf("older index error = %v, want ErrInvalidConfigurationIndex", err)
	}
}

func TestMemoryStorageConfigurationRecordsUseIndependentCopies(t *testing.T) {
	storage := NewMemoryStorage()

	configuration := ConfigurationState{
		OldVoters: []string{"node-A", "node-B"},
	}

	if err := storage.SaveConfigurationAt(5, configuration); err != nil {
		t.Fatal(err)
	}

	configuration.OldVoters[0] = "node-X"

	records, err := storage.LoadConfigurationRecords()
	if err != nil {
		t.Fatal(err)
	}

	if records[0].Configuration.OldVoters[0] != "node-A" {
		t.Fatal("stored configuration changed through input")
	}

	records[0].Configuration.OldVoters[0] = "node-Y"

	again, err := storage.LoadConfigurationRecords()
	if err != nil {
		t.Fatal(err)
	}

	if again[0].Configuration.OldVoters[0] != "node-A" {
		t.Fatal("stored configuration changed through output")
	}
}

func TestDiskStorageConfigurationRecordsSurviveRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft", "hard-state.log")

	first, err := NewDiskStorage(path)
	if err != nil {
		t.Fatal(err)
	}

	config1 := ConfigurationState{
		OldVoters: []string{"node-A", "node-B", "node-C"},
	}
	config2 := ConfigurationState{
		OldVoters: []string{"node-A", "node-B", "node-C"},
		NewVoters: []string{"node-A", "node-B", "node-D"},
	}

	if err := first.SaveConfigurationAt(5, config1); err != nil {
		t.Fatal(err)
	}
	if err := first.SaveConfigurationAt(8, config2); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	second, err := NewDiskStorage(path)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()

	records, err := second.LoadConfigurationRecords()
	if err != nil {
		t.Fatal(err)
	}

	want := []ConfigurationRecord{
		{Index: 5, Configuration: config1},
		{Index: 8, Configuration: config2},
	}

	if !reflect.DeepEqual(records, want) {
		t.Fatalf("records = %+v, want %+v", records, want)
	}
}

func TestDiskStorageConfigurationRecordIsIdempotent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft", "hard-state.log")

	storage, err := NewDiskStorage(path)
	if err != nil {
		t.Fatal(err)
	}
	defer storage.Close()

	config := ConfigurationState{
		OldVoters: []string{"node-A", "node-B"},
	}

	if err := storage.SaveConfigurationAt(5, config); err != nil {
		t.Fatal(err)
	}
	if err := storage.SaveConfigurationAt(5, config); err != nil {
		t.Fatalf("identical retry failed: %v", err)
	}

	records, err := storage.LoadConfigurationRecords()
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 {
		t.Fatalf("got %d records, want 1", len(records))
	}
}

func TestDiskStorageRejectsConfigurationIndexConflict(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft", "hard-state.log")

	storage, err := NewDiskStorage(path)
	if err != nil {
		t.Fatal(err)
	}
	defer storage.Close()

	first := ConfigurationState{
		OldVoters: []string{"node-A", "node-B"},
	}
	second := ConfigurationState{
		OldVoters: []string{"node-A", "node-C"},
	}

	if err := storage.SaveConfigurationAt(5, first); err != nil {
		t.Fatal(err)
	}

	err = storage.SaveConfigurationAt(5, second)
	if !errors.Is(err, ErrConfigurationIndexConflict) {
		t.Fatalf("error = %v, want ErrConfigurationIndexConflict", err)
	}
}

func TestDiskStorageRecoversIncompleteConfigurationRecord(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft", "hard-state.log")

	storage, err := NewDiskStorage(path)
	if err != nil {
		t.Fatal(err)
	}
	defer storage.Close()

	config := ConfigurationState{
		OldVoters: []string{"node-A", "node-B"},
	}

	if err := storage.SaveConfigurationAt(5, config); err != nil {
		t.Fatal(err)
	}

	file, err := os.OpenFile(
		path+".configuration",
		os.O_APPEND|os.O_WRONLY,
		0600,
	)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := file.WriteString(`{"index":6,"configuration":`); err != nil {
		file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	records, err := storage.LoadConfigurationRecords()
	if err != nil {
		t.Fatal(err)
	}

	if len(records) != 1 || records[0].Index != 5 {
		t.Fatalf("records after recovery = %+v, want only index 5", records)
	}

	if err := storage.SaveConfigurationAt(8, config); err != nil {
		t.Fatalf("save after recovery: %v", err)
	}

	records, err = storage.LoadConfigurationRecords()
	if err != nil {
		t.Fatal(err)
	}

	if len(records) != 2 || records[1].Index != 8 {
		t.Fatalf("records after append = %+v, want indexes 5 and 8", records)
	}
}
