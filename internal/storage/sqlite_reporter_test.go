package storage

import "testing"

func TestIsSQLiteBusy(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"locked message", errString("database is locked (5)"), true},
		{"busy code", errString("SQLITE_BUSY: busy"), true},
		{"other", errString("some other error"), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isSQLiteBusy(tc.err); got != tc.want {
				t.Fatalf("isSQLiteBusy(%v)=%v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

type errString string

func (e errString) Error() string { return string(e) }
