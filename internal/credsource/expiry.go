package credsource

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// unixMillisFrom is the smallest Unix time read as milliseconds rather
// than seconds: in seconds it lies in the year 33658.
const unixMillisFrom = 1_000_000_000_000

// ReadExpiry runs command on the host, as [Read] runs a command source,
// and returns when the credential name expires: its standard output, an
// RFC 3339 time or a Unix time in seconds or milliseconds. A command that
// fails, outlives [Bound] or prints anything else is an [*Error].
func ReadExpiry(ctx context.Context, name, command string) (time.Time, error) {
	raw, err := runCommand(ctx, command)
	if err != nil {
		return time.Time{}, &Error{Credential: name, Source: "expires_command", Reason: err.Error(), Err: err}
	}
	at, err := ParseExpiry(string(raw))
	if err != nil {
		return time.Time{}, &Error{Credential: name, Source: "expires_command", Reason: err.Error(), Err: err}
	}
	return at, nil
}

// ParseExpiry reads text, with surrounding white space removed, as an
// RFC 3339 time or a Unix time in seconds or milliseconds.
func ParseExpiry(text string) (time.Time, error) {
	text = strings.TrimSpace(text)
	if text == "" {
		return time.Time{}, fmt.Errorf("printed no expiry")
	}
	if at, err := time.Parse(time.RFC3339Nano, text); err == nil {
		return at.UTC(), nil
	}
	n, err := strconv.ParseInt(text, 10, 64)
	if err != nil || n <= 0 {
		return time.Time{}, fmt.Errorf("printed %q, which is neither an RFC 3339 time nor a Unix time", clip(text))
	}
	if n >= unixMillisFrom {
		return time.UnixMilli(n).UTC(), nil
	}
	return time.Unix(n, 0).UTC(), nil
}

// TokenExpiry returns the expiry a JSON Web Token carries in its exp
// claim, and false when value is not a token with one. The token's
// signature is not checked: the expiry only says when to renew it.
func TokenExpiry(value string) (time.Time, bool) {
	parts := strings.Split(value, ".")
	if len(parts) != 3 {
		return time.Time{}, false
	}
	payload, err := base64.RawURLEncoding.DecodeString(strings.TrimRight(parts[1], "="))
	if err != nil {
		return time.Time{}, false
	}
	var claims struct {
		Exp json.Number `json:"exp"`
	}
	if err := json.Unmarshal(payload, &claims); err != nil || claims.Exp == "" {
		return time.Time{}, false
	}
	exp, err := claims.Exp.Float64()
	if err != nil || exp <= 0 {
		return time.Time{}, false
	}
	return time.Unix(int64(exp), 0).UTC(), true
}

func clip(s string) string {
	const max = 64
	if len(s) <= max {
		return s
	}
	return s[:max] + "..."
}
