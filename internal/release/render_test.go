package release_test

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/relayplane/relayplane/internal/release"
	"go.yaml.in/yaml/v3"
)

func digest(c rune) string { return "sha256:" + strings.Repeat(string(c), 64) }

func refs() map[string]string {
	return map[string]string{
		"gateway":    "ghcr.io/acme/relayplane-gateway@" + digest('a'),
		"worker":     "ghcr.io/acme/relayplane-worker@" + digest('b'),
		"reconciler": "ghcr.io/acme/relayplane-reconciler@" + digest('c'),
		"evolution":  "ghcr.io/acme/relayplane-evolution@" + digest('d'),
	}
}

func template(t *testing.T) []byte {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "deploy", "kubernetes", "relayplane.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// The template in the repository must always be renderable: this is what the release workflow does with real digests.
func TestTheRepositoryManifestRendersToSomethingDeployable(t *testing.T) {
	out, err := release.Render(template(t), refs())
	if err != nil {
		t.Fatal(err)
	}
	text := string(out)
	for c, ref := range refs() {
		if !strings.Contains(text, "image: "+ref) {
			t.Errorf("%s: the pinned reference is not in the manifest", c)
		}
	}
	if strings.Contains(text, "REPLACE_WITH") || strings.Contains(text, "registry.example.com/relayplane") {
		t.Error("a placeholder survived")
	}
	// rendering changes the image lines and nothing else
	in, outLines := strings.Split(string(template(t)), "\n"), strings.Split(text, "\n")
	if len(in) != len(outLines) {
		t.Fatalf("line count changed: %d -> %d", len(in), len(outLines))
	}
	changed := 0
	for i := range in {
		if in[i] != outLines[i] {
			changed++
			if !strings.Contains(in[i], "image:") {
				t.Errorf("line %d changed but is not an image line: %q", i+1, in[i])
			}
		}
	}
	if changed != 4 {
		t.Errorf("exactly the 4 image lines change, got %d", changed)
	}
	if err := release.Verify(out); err != nil {
		t.Error(err)
	}
}

func TestTheRepositoryManifestIsValidYAMLAndKeepsAvailabilityGuards(t *testing.T) {
	raw := template(t)
	dec := yaml.NewDecoder(bytes.NewReader(raw))
	documents := 0
	for {
		var document map[string]any
		err := dec.Decode(&document)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("manifest document %d is invalid YAML: %v", documents+1, err)
		}
		if len(document) != 0 {
			documents++
		}
	}
	if documents != 12 {
		t.Fatalf("expected 12 Kubernetes resources, got %d", documents)
	}

	text := string(raw)
	if !strings.Contains(text, "updateStrategy: {type: OnDelete}") {
		t.Error("Evolution must require an explicit, drained pod replacement")
	}
	if got := strings.Count(text, "kind: PodDisruptionBudget"); got != 4 {
		t.Errorf("expected four PodDisruptionBudgets, got %d", got)
	}
}

func TestRenderRefusesWhatIsNotImmutable(t *testing.T) {
	for name, mod := range map[string]func(map[string]string){
		"a tag":              func(m map[string]string) { m["gateway"] = "ghcr.io/acme/relayplane-gateway:1.2.0" },
		"latest":             func(m map[string]string) { m["worker"] = "ghcr.io/acme/relayplane-worker:latest" },
		"a short digest":     func(m map[string]string) { m["reconciler"] = "ghcr.io/acme/x@sha256:abc" },
		"the placeholder":    func(m map[string]string) { m["evolution"] = "ghcr.io/acme/x@sha256:REPLACE_WITH_BUILD_DIGEST" },
		"a missing image":    func(m map[string]string) { delete(m, "worker") },
		"an unknown service": func(m map[string]string) { m["mystery"] = "ghcr.io/acme/x@" + digest('e') },
	} {
		m := refs()
		mod(m)
		if _, err := release.Render(template(t), m); err == nil {
			t.Errorf("%s must be refused", name)
		}
	}
}

func TestVerifyCatchesWhatSlipsThrough(t *testing.T) {
	good := "containers:\n  - name: a\n    image: ghcr.io/acme/a@" + digest('a') + "  # comment\n"
	if err := release.Verify([]byte(good)); err != nil {
		t.Errorf("a pinned image with a comment: %v", err)
	}
	for name, doc := range map[string]string{
		"a tag":         "    image: ghcr.io/acme/a:1.0\n",
		"a placeholder": "    image: registry.example.com/relayplane/gateway:0.1.0@sha256:REPLACE_WITH_BUILD_DIGEST\n",
		"in a list":     "      - image: redis:7\n",
	} {
		if err := release.Verify([]byte(doc)); err == nil {
			t.Errorf("%s must be refused", name)
		}
	}
	if err := release.Verify([]byte("# image: registry.example.com/x  REPLACE_WITH_BUILD_DIGEST\n")); err != nil {
		t.Errorf("comments are the template explaining itself: %v", err)
	}
}

// A template with an image the renderer was never told about must not render with a hole in it.
func TestRenderRefusesATemplateWithAnUnknownImage(t *testing.T) {
	tpl := string(template(t)) + "\n          image: registry.example.com/relayplane/newthing:0.1.0@sha256:REPLACE_WITH_BUILD_DIGEST\n"
	if _, err := release.Render([]byte(tpl), refs()); err == nil {
		t.Error("an image without a build must stop the release")
	}
}
