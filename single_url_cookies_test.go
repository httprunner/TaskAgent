package pool

import (
	"context"
	"errors"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/httprunner/TaskAgent/pkg/feishu"
)

func TestCookieProviderRotatesValues(t *testing.T) {
	rows := []feishu.CookieRow{
		{RecordID: "rec-1", Cookies: "cookie-1"},
		{RecordID: "rec-2", Cookies: "cookie-2"},
	}
	prov := &cookieProvider{
		tableURL: "https://bitable.example",
		platform: defaultCookiePlatform,
		cacheTTL: time.Hour,
		rand:     nil,
	}
	prov.fetchFn = func(context.Context, *feishu.Client, string, string) ([]feishu.CookieRow, error) {
		return rows, nil
	}
	prov.validate = func(context.Context, string) error { return nil }
	prov.markInvalid = func(context.Context, string) error { return nil }

	ctx := context.Background()
	first, err := prov.PickCookie(ctx)
	if err != nil || first == nil || first.Value != "cookie-1" {
		t.Fatalf("unexpected first cookie: %#v err=%v", first, err)
	}
	second, err := prov.PickCookie(ctx)
	if err != nil || second == nil || second.Value != "cookie-2" {
		t.Fatalf("unexpected second cookie: %#v err=%v", second, err)
	}
	third, err := prov.PickCookie(ctx)
	if err != nil || third == nil || third.Value != "cookie-1" {
		t.Fatalf("expected rotation back to cookie-1, got %#v err=%v", third, err)
	}
}

func TestCookieProviderReturnsNilWhenEmpty(t *testing.T) {
	prov := &cookieProvider{
		tableURL: "https://bitable.example",
		platform: defaultCookiePlatform,
		cacheTTL: time.Second,
		rand:     rand.New(rand.NewSource(1)),
	}
	prov.fetchFn = func(context.Context, *feishu.Client, string, string) ([]feishu.CookieRow, error) {
		return nil, nil
	}
	prov.validate = func(context.Context, string) error { return nil }
	prov.markInvalid = func(context.Context, string) error { return nil }

	ctx := context.Background()
	rec, err := prov.PickCookie(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rec != nil {
		t.Fatalf("expected nil cookie, got %#v", rec)
	}
}

func TestCookieProviderDropsInvalidCookies(t *testing.T) {
	rows := []feishu.CookieRow{
		{RecordID: "rec-A", Cookies: "cookie-A"},
		{RecordID: "rec-B", Cookies: "cookie-B"},
	}
	prov := &cookieProvider{
		tableURL: "https://bitable.example",
		platform: defaultCookiePlatform,
		cacheTTL: time.Hour,
		rand:     nil,
	}
	prov.fetchFn = func(context.Context, *feishu.Client, string, string) ([]feishu.CookieRow, error) {
		return rows, nil
	}
	failOnce := true
	prov.validate = func(_ context.Context, value string) error {
		if failOnce && strings.Contains(value, "A") {
			failOnce = false
			return errors.New("invalid cookie")
		}
		return nil
	}
	var marked []string
	prov.markInvalid = func(_ context.Context, recordID string) error {
		marked = append(marked, recordID)
		return nil
	}

	ctx := context.Background()
	rec, err := prov.PickCookie(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rec == nil || rec.Value != "cookie-B" {
		t.Fatalf("expected fallback to cookie-B, got %#v", rec)
	}
	if len(marked) != 1 || marked[0] != "rec-A" {
		t.Fatalf("expected rec-A to be marked invalid, got %#v", marked)
	}
}
