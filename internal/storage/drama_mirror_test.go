package storage

import (
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/httprunner/TaskAgent/internal/feishusdk"
)

func TestMirrorDramaRowsWritesToSQLite(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "tracking.sqlite")
	t.Setenv("TRACKING_STORAGE_DB_PATH", dbPath)
	dramaURL := "https://example.feishusdk.cn/base/appxxx?table=tbl123"
	t.Setenv("DRAMA_BITABLE_URL", dramaURL)
	t.Setenv("DRAMA_SQLITE_TABLE", "drama_test")
	ResetDramaMirrorForTest()
	t.Cleanup(ResetDramaMirrorForTest)

	rows := []feishusdk.BitableRow{
		{
			RecordID: "rec1",
			Fields: map[string]any{
				feishusdk.DefaultDramaFields.DramaName:                "热门短剧",
				feishusdk.DefaultDramaFields.DramaID:                  "drama-1",
				feishusdk.DefaultDramaFields.TotalDuration:            float64(3600),
				feishusdk.DefaultDramaFields.EpisodeCount:             float64(24),
				feishusdk.DefaultDramaFields.Priority:                 "P1",
				feishusdk.DefaultDramaFields.RightsProtectionScenario: "测试场景",
			},
		},
	}

	MirrorDramaRowsIfNeeded(dramaURL, rows)

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open sqlite failed: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	query := `SELECT "` + feishusdk.DefaultDramaFields.DramaName + `", "` + feishusdk.DefaultDramaFields.DramaID + `", "` + feishusdk.DefaultDramaFields.TotalDuration + `", "` + feishusdk.DefaultDramaFields.EpisodeCount + `", "` + feishusdk.DefaultDramaFields.Priority + `", "` + feishusdk.DefaultDramaFields.RightsProtectionScenario + `" FROM "drama_test" WHERE "` + feishusdk.DefaultDramaFields.DramaName + `" = ?`
	row := db.QueryRow(query, "热门短剧")
	var (
		name, id, priority, scenario sql.NullString
		duration, episodes           sql.NullFloat64
	)
	if err := row.Scan(&name, &id, &duration, &episodes, &priority, &scenario); err != nil {
		t.Fatalf("scan drama row failed: %v", err)
	}
	if name.String != "热门短剧" || id.String != "drama-1" {
		t.Fatalf("unexpected name/id: %s/%s", name.String, id.String)
	}
	if !duration.Valid || duration.Float64 != 3600 {
		t.Fatalf("unexpected duration: %+v", duration)
	}
	if !episodes.Valid || episodes.Float64 != 24 {
		t.Fatalf("unexpected episodes: %+v", episodes)
	}
	if priority.String != "P1" || scenario.String != "测试场景" {
		t.Fatalf("unexpected priority/rights: %s/%s", priority.String, scenario.String)
	}
}
