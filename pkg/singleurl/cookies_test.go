package singleurl

import (
	"context"
	"errors"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/httprunner/TaskAgent/internal/feishusdk"
)

func TestCookieProviderRotatesValues(t *testing.T) {
	rows := []feishusdk.CookieRow{
		{RecordID: "rec-1", Cookies: "cookie-1"},
		{RecordID: "rec-2", Cookies: "cookie-2"},
	}
	prov := &cookieProvider{
		tableURL: "https://bitable.example",
		platform: defaultCookiePlatform,
		cacheTTL: time.Hour,
		rand:     nil,
	}
	prov.fetchFn = func(context.Context, *feishusdk.Client, string, string) ([]feishusdk.CookieRow, error) {
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
	prov.fetchFn = func(context.Context, *feishusdk.Client, string, string) ([]feishusdk.CookieRow, error) {
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
	rows := []feishusdk.CookieRow{
		{RecordID: "rec-A", Cookies: "cookie-A"},
		{RecordID: "rec-B", Cookies: "cookie-B"},
	}
	prov := &cookieProvider{
		tableURL: "https://bitable.example",
		platform: defaultCookiePlatform,
		cacheTTL: time.Hour,
		rand:     nil,
	}
	prov.fetchFn = func(context.Context, *feishusdk.Client, string, string) ([]feishusdk.CookieRow, error) {
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

func TestCookieProviderSkipsUnavailableValidation(t *testing.T) {
	rows := []feishusdk.CookieRow{
		{RecordID: "rec-A", Cookies: "cookie-A"},
		{RecordID: "rec-B", Cookies: "cookie-B"},
	}
	prov := &cookieProvider{
		tableURL: "https://bitable.example",
		platform: defaultCookiePlatform,
		cacheTTL: time.Hour,
		rand:     nil,
	}
	prov.fetchFn = func(context.Context, *feishusdk.Client, string, string) ([]feishusdk.CookieRow, error) {
		return rows, nil
	}
	first := true
	prov.validate = func(_ context.Context, value string) error {
		if strings.Contains(value, "A") && first {
			first = false
			return errCookieValidationUnavailable
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
	if len(marked) != 0 {
		t.Fatalf("expected no invalidation when validation is unavailable, got %#v", marked)
	}
}

func TestKuaishouCookieValidatorFallsBackToHomepageValid(t *testing.T) {
	var homepageCookie string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		homepageCookie = r.Header.Get("Cookie")
		w.Header().Set("Content-Type", "text/html")
		_, _ = io.WriteString(w, testKuaishouHomepageWithProfile)
	}))
	defer server.Close()
	validator := &kuaishouCookieValidator{
		client:      server.Client(),
		homepageURL: server.URL,
		userAgent:   "test-agent",
	}
	if err := validator.Validate(context.Background(), "foo=bar"); err != nil {
		t.Fatalf("expected homepage fallback success, got %v", err)
	}
	if homepageCookie != "foo=bar" {
		t.Fatalf("expected cookie header forwarded to homepage, got %q", homepageCookie)
	}
}

func TestKuaishouCookieValidatorHomepageInvalid(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		_, _ = io.WriteString(w, testKuaishouHomepageWithoutProfile)
	}))
	defer server.Close()
	validator := &kuaishouCookieValidator{
		client:      server.Client(),
		homepageURL: server.URL,
	}
	if err := validator.Validate(context.Background(), "foo=bar"); !errors.Is(err, errCookieInvalid) {
		t.Fatalf("expected errCookieInvalid, got %v", err)
	}
}

func TestKuaishouCookieValidatorHomepageEmptyProfileInvalid(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		_, _ = io.WriteString(w, testKuaishouHomepageWithEmptyProfile)
	}))
	defer server.Close()
	validator := &kuaishouCookieValidator{
		client:      server.Client(),
		homepageURL: server.URL,
	}
	if err := validator.Validate(context.Background(), "foo=bar"); !errors.Is(err, errCookieInvalid) {
		t.Fatalf("expected errCookieInvalid, got %v", err)
	}
}

func TestKuaishouCookieValidatorHomepageUnavailable(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()
	validator := &kuaishouCookieValidator{
		client:      server.Client(),
		homepageURL: server.URL,
	}
	if err := validator.Validate(context.Background(), "foo=bar"); !errors.Is(err, errCookieValidationUnavailable) {
		t.Fatalf("expected errCookieValidationUnavailable, got %v", err)
	}
}

func TestKuaishouCookieValidatorHomepageUnauthorized(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer server.Close()
	validator := &kuaishouCookieValidator{
		client:      server.Client(),
		homepageURL: server.URL,
	}
	if err := validator.Validate(context.Background(), "foo=bar"); !errors.Is(err, errCookieInvalid) {
		t.Fatalf("expected errCookieInvalid, got %v", err)
	}
}

func TestExtractApolloStateJSONWithTrailingScript(t *testing.T) {
	body := []byte(`<script>window.__APOLLO_STATE__={"foo":{"bar":"baz"}};window.__INITIAL_STATE__={"hello":"world"};(function(){})();</script>`)
	payload, err := extractApolloStateJSON(body)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := `{"foo":{"bar":"baz"}}`
	if string(payload) != expected {
		t.Fatalf("unexpected payload: %s", payload)
	}
}

const (
	testKuaishouHomepageWithProfile      = `<!DOCTYPE html><html><head></head><body><script>window.__APOLLO_STATE__={"defaultClient":{"$ROOT_QUERY.visionProfile({})":{"result":1,"userProfile":{"profile":{"user_id":"valid"}}}}};window.__INITIAL_STATE__={"foo":"bar"};(function(){})();</script></body></html>`
	testKuaishouHomepageWithoutProfile   = `<!DOCTYPE html><html><head></head><body><script>window.__APOLLO_STATE__={"defaultClient":{"ROOT_QUERY":{}}};window.__INITIAL_STATE__={"foo":"bar"};(function(){})();</script></body></html>`
	testKuaishouHomepageWithEmptyProfile = `<!DOCTYPE html><html><head></head><body><script>window.__APOLLO_STATE__={"defaultClient":{"$ROOT_QUERY.visionProfile({})":{"result":21,"userProfile":null}}};window.__INITIAL_STATE__={"foo":"bar"};(function(){})();</script></body></html>`
)
