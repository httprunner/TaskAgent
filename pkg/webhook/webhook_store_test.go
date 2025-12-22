package webhook

import "testing"

func TestEncodeTaskIDsForFeishu(t *testing.T) {
	cases := []struct {
		name string
		in   []int64
		want string
	}{
		{name: "empty", in: nil, want: ""},
		{name: "single", in: []int64{123}, want: "123"},
		{name: "multiple", in: []int64{123, 456}, want: "123,456"},
		{name: "dedupe_and_filter_non_positive", in: []int64{0, 123, 123, -1, 456}, want: "123,456"},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			got := encodeTaskIDsForFeishu(tc.in)
			if got != tc.want {
				t.Fatalf("got=%q want=%q", got, tc.want)
			}
		})
	}
}
