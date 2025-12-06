package piracy

import (
	"testing"

	pool "github.com/httprunner/TaskAgent"
	"github.com/httprunner/TaskAgent/pkg/feishu"
)

func TestParamsFromFeishuTasksSuccess(t *testing.T) {
	tasks := []*pool.FeishuTask{
		{Params: "A", Status: feishu.StatusSuccess},
		{Params: "B", Status: feishu.StatusSuccess},
		{Params: "A", Status: feishu.StatusSuccess},
	}
	params, ok := paramsFromFeishuTasks(tasks)
	if !ok {
		t.Fatalf("expected params to be ready when all successes")
	}
	if len(params) != 2 {
		t.Fatalf("unexpected param count: %v", params)
	}
	if params[0] != "A" || params[1] != "B" {
		t.Fatalf("unexpected params order: %v", params)
	}
}

func TestParamsFromFeishuTasksRejectsPending(t *testing.T) {
	tasks := []*pool.FeishuTask{{Params: "A", Status: feishu.StatusPending}}
	if _, ok := paramsFromFeishuTasks(tasks); ok {
		t.Fatalf("expected pending status to block detection")
	}
}

func TestCaptureTasksAllSuccess(t *testing.T) {
	rows := []captureTaskRow{{Status: feishu.StatusSuccess}, {Status: feishu.StatusSuccess}}
	if !captureTasksAllSuccess(rows) {
		t.Fatalf("expected all success to return true")
	}
	rows = append(rows, captureTaskRow{Status: feishu.StatusPending})
	if captureTasksAllSuccess(rows) {
		t.Fatalf("expected pending status to fail readiness")
	}
}

func TestParamsFromCaptureTasksDedupes(t *testing.T) {
	rows := []captureTaskRow{{Params: "Foo"}, {Params: "Foo"}, {Params: "Bar"}}
	params := paramsFromCaptureTasks(rows)
	if len(params) != 2 {
		t.Fatalf("unexpected param count: %v", params)
	}
	if params[0] != "Foo" || params[1] != "Bar" {
		t.Fatalf("unexpected params: %v", params)
	}
}
