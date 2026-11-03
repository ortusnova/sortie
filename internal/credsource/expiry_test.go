package credsource

import (
	"context"
	"encoding/base64"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func TestParseExpiry(t *testing.T) {
	t.Parallel()

	want := time.Date(2026, 11, 3, 12, 30, 0, 0, time.UTC)
	for _, text := range []string{
		"2026-11-03T12:30:00Z",
		"2026-11-03T13:30:00+01:00\n",
		"1793709000",
		"  1793709000000  ",
	} {
		got, err := ParseExpiry(text)
		if err != nil || !got.Equal(want) {
			t.Errorf("ParseExpiry(%q) = %v, %v, want %v", text, got, err, want)
		}
	}
	for _, text := range []string{"", "null", "-5", "soon"} {
		if got, err := ParseExpiry(text); err == nil {
			t.Errorf("ParseExpiry(%q) = %v, want an error", text, got)
		}
	}
}

func TestReadExpiry_RunsTheCommandAnewEachTime(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "expires")
	command := "cat '" + path + "'"

	writeFile(t, path, "1793709000000\n")
	got, err := ReadExpiry(context.Background(), "claude", command)
	if err != nil || !got.Equal(time.UnixMilli(1793709000000)) {
		t.Fatalf("ReadExpiry() = %v, %v, want the first expiry", got, err)
	}

	writeFile(t, path, "2026-11-04T00:00:00Z")
	got, err = ReadExpiry(context.Background(), "claude", command)
	if err != nil || !got.Equal(time.Date(2026, 11, 4, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("ReadExpiry() after the renewal = %v, %v, want the renewed expiry", got, err)
	}
}

func TestReadExpiry_FailureNamesTheCredential(t *testing.T) {
	t.Parallel()

	for _, command := range []string{"exit 3", "echo later"} {
		_, err := ReadExpiry(context.Background(), "claude", command)
		var credErr *Error
		if !errors.As(err, &credErr) || credErr.Credential != "claude" || credErr.Source != "expires_command" {
			t.Errorf("ReadExpiry(%q) error = %v, want an *Error naming claude and expires_command", command, err)
		}
	}
}

func TestTokenExpiry(t *testing.T) {
	t.Parallel()

	jwt := func(payload string) string {
		return "eyJhbGciOiJIUzI1NiJ9." + base64.RawURLEncoding.EncodeToString([]byte(payload)) + ".c2ln"
	}
	if got, ok := TokenExpiry(jwt(`{"sub":"sortie","exp":1793709000}`)); !ok || !got.Equal(time.Unix(1793709000, 0)) {
		t.Errorf("TokenExpiry(token with exp) = %v, %v, want its exp", got, ok)
	}
	for _, value := range []string{"sk-ant-oat01-opaque", jwt(`{"sub":"sortie"}`), "a.b.c", jwt(`not json`)} {
		if got, ok := TokenExpiry(value); ok {
			t.Errorf("TokenExpiry(%q) = %v, true, want false", value, got)
		}
	}
}
