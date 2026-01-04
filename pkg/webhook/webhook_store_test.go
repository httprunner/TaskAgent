package webhook

import "testing"

func TestEncodeTaskIDsByStatusForFeishu(t *testing.T) {
	cases := []struct {
		name string
		in   map[string][]int64
		want string
	}{
		{name: "empty", in: nil, want: ""},
		{name: "pending_single", in: map[string][]int64{"pending": []int64{123}}, want: "{\"pending\":[123]}"},
		{name: "normalize_and_sort", in: map[string][]int64{"SUCCESS": []int64{456, 123, 123, 0}, "failed": []int64{999, -1}}, want: "{\"failed\":[999],\"success\":[123,456]}"},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			got := encodeTaskIDsByStatusForFeishu(tc.in)
			if got != tc.want {
				t.Fatalf("got=%q want=%q", got, tc.want)
			}
		})
	}
}
