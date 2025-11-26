package piracy

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/httprunner/TaskAgent/pkg/storage"
)

func TestFetchSQLiteParams(t *testing.T) {
	t.Helper()
	t.Setenv("TRACKING_STORAGE_DB_PATH", filepath.Join(t.TempDir(), "capture.sqlite"))
	cfg := Config{}
	cfg.ApplyDefaults()

	path, err := storage.ResolveDatabasePath()
	if err != nil {
		t.Fatalf("resolve db path failed: %v", err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open sqlite failed: %v", err)
	}
	defer db.Close()

	schema := `CREATE TABLE capture_results (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		Params TEXT,
		UserID TEXT
	);`
	if _, err := db.Exec(schema); err != nil {
		t.Fatalf("create table failed: %v", err)
	}

	seed := `INSERT INTO capture_results (Params, UserID) VALUES
		('drama-one', 'u1'),
		('drama-two', 'u2'),
		('drama-one', 'u3'),
		('   ', 'u4'),
		('', 'u5');`
	if _, err := db.Exec(seed); err != nil {
		t.Fatalf("seed data failed: %v", err)
	}

	params, err := FetchSQLiteParams(context.Background(), cfg, path)
	if err != nil {
		t.Fatalf("FetchSQLiteParams returned error: %v", err)
	}

	want := []string{"drama-one", "drama-two"}
	if len(params) != len(want) {
		t.Fatalf("unexpected params length: got %d want %d", len(params), len(want))
	}
	for i, v := range want {
		if params[i] != v {
			t.Fatalf("params[%d] = %s, want %s", i, params[i], v)
		}
	}
}
