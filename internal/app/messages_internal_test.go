package app

import (
	"strings"
	"testing"

	"github.com/relayplane/relayplane/internal/core/messaging"
)

func TestClipQuoteKeepsWholeCharacters(t *testing.T) {
	long := strings.Repeat("ã", 5000)
	got := clipQuote(long)
	if n := len([]rune(got)); n != messaging.MaxQuotePreview {
		t.Errorf("clipped to %d characters, want %d", n, messaging.MaxQuotePreview)
	}
	if strings.ContainsRune(got, '�') {
		t.Error("clipping must never cut a character in half")
	}
	if clipQuote("curto") != "curto" || clipQuote("") != "" {
		t.Error("short previews are untouched")
	}
}
