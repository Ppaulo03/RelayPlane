package media

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/relayplane/relayplane/internal/core/errs"
)

var sha = strings.Repeat("a", 64)

func TestOwnedBy_TenantIsolation(t *testing.T) {
	cases := []struct {
		tenant, key string
		ok          bool
	}{
		{"t1", "t1/media/m1/doc.pdf", true},
		{"t1", "t2/media/m1/doc.pdf", false},
		{"t1", "t10/media/m1/doc.pdf", false}, // prefix collision
		{"t1", "t1/../t2/media/x", false},
		{"t1", "t1/", false},
		{"t1", "/t1/media/x", false},
		{"", "/x", false},
		{"t/1", "t/1/x", false},
	}
	for _, c := range cases {
		if got := OwnedBy(c.tenant, c.key); got != c.ok {
			t.Errorf("OwnedBy(%q,%q)=%v want %v", c.tenant, c.key, got, c.ok)
		}
	}
}

func TestObjectKeyIsOwnedAndSanitised(t *testing.T) {
	k := ObjectKey("t1", "m1", "../../etc/passwd")
	if !OwnedBy("t1", k) || strings.Contains(k, "..") {
		t.Fatalf("unsafe key %q", k)
	}
	if got := SafeFilename(""); got != "file" {
		t.Errorf("empty name -> %q", got)
	}
}

func TestValidateRef(t *testing.T) {
	now := time.Now()
	p := DefaultPolicy()
	good := Ref{ObjectKey: "t1/media/m/a.pdf", ContentType: "application/pdf", Size: 1000, SHA256: sha, ExpiresAt: now.Add(time.Hour)}
	if err := ValidateRef("t1", good, p, now); err != nil {
		t.Fatalf("good ref: %v", err)
	}
	mut := map[string]func(*Ref){
		"other tenant": func(r *Ref) { r.ObjectKey = "t2/media/m/a.pdf" },
		"too big":      func(r *Ref) { r.Size = p.MaxBytes + 1 },
		"zero size":    func(r *Ref) { r.Size = 0 },
		"bad mime":     func(r *Ref) { r.ContentType = "application/x-msdownload" },
		"bad sha":      func(r *Ref) { r.SHA256 = "XYZ" },
		"expired":      func(r *Ref) { r.ExpiresAt = now.Add(-time.Second) },
	}
	for name, m := range mut {
		r := good
		m(&r)
		if err := ValidateRef("t1", r, p, now); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
	other := good
	other.ObjectKey = "t2/x/y"
	if err := ValidateRef("t1", other, p, now); !errors.Is(err, errs.ErrForbidden) {
		t.Errorf("cross-tenant reference must be ErrForbidden, got %v", err)
	}
}

func TestTypeAllowedWildcards(t *testing.T) {
	p := DefaultPolicy()
	for _, ok := range []string{"image/png", "video/mp4", "audio/ogg; codecs=opus", "application/pdf"} {
		if !p.TypeAllowed(ok) {
			t.Errorf("%s should be allowed", ok)
		}
	}
	for _, bad := range []string{"application/x-sh", "", "text/html"} {
		if p.TypeAllowed(bad) {
			t.Errorf("%q should be rejected", bad)
		}
	}
}

func TestEnforceInlineLimit(t *testing.T) { // INV-11
	if err := EnforceInlineLimit(make([]byte, 100), 256); err != nil {
		t.Fatal(err)
	}
	if err := EnforceInlineLimit(make([]byte, 257), 256); !errors.Is(err, errs.ErrPayloadTooLarge) {
		t.Fatalf("want ErrPayloadTooLarge, got %v", err)
	}
}
