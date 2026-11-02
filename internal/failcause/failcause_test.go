package failcause

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/sortie-ai/sortie/internal/redact"
)

type carrier struct{ output string }

func (c *carrier) Error() string         { return "step failed" }
func (c *carrier) FailureOutput() string { return c.output }

var _ Carrier = (*carrier)(nil)

func TestOf_FindsTheCarrierInTheChain(t *testing.T) {
	t.Parallel()

	inner := &carrier{output: "  the whole cause\n"}
	err := fmt.Errorf("export: %w", errors.Join(errors.New("other"), fmt.Errorf("push: %w", inner)))
	if got := Of(err); got != "the whole cause" {
		t.Errorf("Of() = %q, want %q", got, "the whole cause")
	}
}

func TestOf_NoCarrier(t *testing.T) {
	t.Parallel()

	for _, err := range []error{nil, errors.New("plain"), &carrier{output: " \n"}} {
		if got := Of(err); got != "" {
			t.Errorf("Of(%v) = %q, want empty", err, got)
		}
	}
}

func TestOf_MasksRegisteredCredentials(t *testing.T) {
	t.Parallel()

	const secret = "failcause-test-credential-5d1e"
	redact.Add("test.failcause", secret)
	got := Of(&carrier{output: "remote: token " + secret + " lacks scope"})
	if strings.Contains(got, secret) || !strings.Contains(got, "remote: token "+redact.Marker+" lacks scope") {
		t.Errorf("Of() = %q, want the credential masked", got)
	}
}

func TestOf_CapsKeepingStartAndEnd(t *testing.T) {
	t.Parallel()

	const secret = "failcause-test-credential-9a7c"
	redact.Add("test.failcause cap", secret)
	output := "first line\n" + strings.Repeat("é progress\n", 4*MaxBytes) + secret + "\nlast line"
	got := Of(&carrier{output: output})
	if len(got) > MaxBytes {
		t.Errorf("len(Of()) = %d, want at most %d", len(got), MaxBytes)
	}
	if !strings.HasPrefix(got, "first line\n") || !strings.HasSuffix(got, redact.Marker+"\nlast line") {
		t.Errorf("Of() = %q...%q, want the start and the masked end kept", got[:32], got[len(got)-32:])
	}
	if !strings.Contains(got, " bytes cut]\n") {
		t.Error("Of() names no cut")
	}
	if !utf8.ValidString(got) {
		t.Error("Of() is not valid UTF-8")
	}
}

func TestCap_ShortTextUnchanged(t *testing.T) {
	t.Parallel()

	s := strings.Repeat("x", MaxBytes)
	if got := Cap(s); got != s {
		t.Errorf("Cap() changed a text of %d bytes", len(s))
	}
}
