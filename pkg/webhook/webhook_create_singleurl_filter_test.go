package webhook

import (
	"testing"

	taskagent "github.com/httprunner/TaskAgent"
)

func TestBuildSingleURLCaptureTaskFilter_DoesNotUseUnaryOperators(t *testing.T) {
	filter := buildSingleURLCaptureTaskFilter(taskagent.DefaultTaskFields(), "", "")
	if filter == nil {
		t.Fatalf("expected filter")
	}
	for _, cond := range filter.Conditions {
		if cond == nil || cond.Operator == nil {
			continue
		}
		switch *cond.Operator {
		case "isNotEmpty", "isEmpty":
			t.Fatalf("unexpected operator: %s", *cond.Operator)
		}
	}
}

func TestBuildSingleURLCaptureTaskFilter_DoesNotConstrainStatus(t *testing.T) {
	filter := buildSingleURLCaptureTaskFilter(taskagent.DefaultTaskFields(), "", "")
	if filter == nil {
		t.Fatalf("expected filter")
	}
	if len(filter.Children) != 0 {
		t.Fatalf("expected no children filter, got %d", len(filter.Children))
	}
}
