package storage

import (
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/httprunner/TaskAgent/pkg/feishu"
)

func TestMirrorDramaRowsWritesToSQLite(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "tracking.sqlite")
	t.Setenv("TRACKING_STORAGE_DB_PATH", dbPath)
	dramaURL := "https://example.feishu.cn/base/appxxx?table=tbl123"
	t.Setenv("DRAMA_BITABLE_URL", dramaURL)
	t.Setenv("DRAMA_SQLITE_TABLE", "drama_test")
	ResetDramaMirrorForTest()
	t.Cleanup(ResetDramaMirrorForTest)

	rows := []feishu.BitableRow{
		{
			RecordID: "rec1",
			Fields: map[string]any{
				feishu.DefaultDramaFields.DramaName:                "热门短剧",
				feishu.DefaultDramaFields.DramaID:                  "drama-1",
				feishu.DefaultDramaFields.TotalDuration:            float64(3600),
				feishu.DefaultDramaFields.EpisodeCount:             float64(24),
				feishu.DefaultDramaFields.Priority:                 "P1",
				feishu.DefaultDramaFields.RightsProtectionScenario: "测试场景",
			},
		},
	}

	MirrorDramaRowsIfNeeded(dramaURL, rows)

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open sqlite failed: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	query := `SELECT "` + feishu.DefaultDramaFields.DramaName + `", "` + feishu.DefaultDramaFields.DramaID + `", "` + feishu.DefaultDramaFields.TotalDuration + `", "` + feishu.DefaultDramaFields.EpisodeCount + `", "` + feishu.DefaultDramaFields.Priority + `", "` + feishu.DefaultDramaFields.RightsProtectionScenario + `" FROM "drama_test" WHERE "` + feishu.DefaultDramaFields.DramaName + `" = ?`
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
