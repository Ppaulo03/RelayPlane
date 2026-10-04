package archtest

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

func readRepoFile(t *testing.T, rel ...string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(append([]string{repoRoot(t)}, rel...)...))
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// Every image the release publishes goes through a scan first: trivy must come before the step with `push: true`, in each
// job that has one.
func TestReleaseWorkflowScansEveryImageBeforePushingIt(t *testing.T) {
	s := readRepoFile(t, ".github", "workflows", "release.yml")
	jobs := regexp.MustCompile(`(?m)^  [a-z][a-z-]*:\n`).Split(s, -1)
	pushing := 0
	for _, j := range jobs {
		push := strings.Index(j, "push: true")
		if push < 0 {
			continue
		}
		pushing++
		if scan := strings.Index(j, "trivy-action"); scan < 0 || scan > push {
			t.Errorf("a job that pushes an image must scan it (trivy) before the push step:\n%.300s", j)
		}
		if !strings.Contains(j, "provenance:") || !strings.Contains(j, "sbom: true") {
			t.Error("published images carry build provenance and an SBOM")
		}
	}
	if pushing == 0 {
		t.Error("the release workflow publishes nothing: it has no `push: true`")
	}
	if !strings.Contains(s, "./.github/workflows/evolution-image.yml") {
		t.Error("the Evolution image must be produced by its own workflow (one way to build it), called from the release")
	}
}

// A release can only produce a manifest in which every image is pinned: the renderer is in the path and nothing writes the
// manifest around it.
func TestReleaseRendersTheManifestThroughTheCheckedRenderer(t *testing.T) {
	s := readRepoFile(t, ".github", "workflows", "release.yml")
	if !strings.Contains(s, "./cmd/render-release") {
		t.Error("the release must render deploy/kubernetes/relayplane.yaml with cmd/render-release (it refuses placeholders and unpinned images)")
	}
	if strings.Contains(s, "sed -i") || strings.Contains(s, "REPLACE_WITH_BUILD_DIGEST") {
		t.Error("do not patch the manifest by hand in the workflow")
	}
	if !strings.Contains(s, "staging-smoke") {
		t.Error("a release must start from the published images in the production configuration and pass the smoke test")
	}
}

// Staging rehearses production: production mode, no build, and images that come from the release.
func TestStagingIsTheProductionConfigurationFromPinnedImages(t *testing.T) {
	compose := readRepoFile(t, "deploy", "staging", "compose.staging.yml")
	for _, v := range []string{"GATEWAY_IMAGE", "WORKER_IMAGE", "RECONCILER_IMAGE", "EVOLUTION_IMAGE"} {
		if !strings.Contains(compose, "${"+v+":?") {
			t.Errorf("compose.staging.yml must require %s (no default: a staging without a release is not a staging)", v)
		}
	}
	if strings.Count(compose, "build: !reset null") < 5 {
		t.Error("staging must not build anything: every service resets its build")
	}
	script := readRepoFile(t, "deploy", "staging", "staging.sh")
	for _, want := range []string{"APP_ENV=production", "--no-build", "must be pinned by digest"} {
		if !strings.Contains(script, want) {
			t.Errorf("staging.sh lost %q", want)
		}
	}
	if regexp.MustCompile(`(?m)^[^#]*WEBHOOKS_ALLOW_(PRIVATE_DESTINATIONS|INSECURE)=(1|true)`).MatchString(script + compose) {
		t.Error("staging must not relax the production webhook guards")
	}
}

// A node is a stateful singleton: its database holds the WhatsApp sessions. Every pod of the StatefulSet must therefore get its OWN
// database, derived from its stable name, never one shared URI (two processes on one session break it, and a replacement pod keeps
// its name, hence its database, hence its sessions).
func TestEvolutionStatefulSetGivesEachPodItsOwnDatabase(t *testing.T) {
	s := readRepoFile(t, "deploy", "kubernetes", "relayplane.yaml")
	i := strings.Index(s, "kind: StatefulSet")
	if i < 0 {
		t.Fatal("no StatefulSet for the provider nodes")
	}
	sts := s[i:]
	if !regexp.MustCompile(`name: DATABASE_CONNECTION_URI, value: "[^"]*\$\(POD_NAME\)[^"]*"`).MatchString(sts) {
		t.Error("DATABASE_CONNECTION_URI must be derived from $(POD_NAME): one database per node")
	}
	if regexp.MustCompile(`name: DATABASE_CONNECTION_URI, valueFrom`).MatchString(sts) {
		t.Error("DATABASE_CONNECTION_URI must not come whole from a Secret: every pod would share one database")
	}
	if !strings.Contains(sts, "fieldPath: metadata.name") {
		t.Error("POD_NAME must come from the pod's own name")
	}
}

// The runbooks the alerts and docs point to must exist.
func TestRunbooksExist(t *testing.T) {
	for _, f := range []string{"UNKNOWN-MESSAGES.md", "NODE-REPLACEMENT.md"} {
		if len(readRepoFile(t, "docs", "runbooks", f)) < 500 {
			t.Errorf("runbook %s is empty", f)
		}
	}
	if !strings.Contains(readRepoFile(t, "deploy", "prometheus", "alerts.yml"), "docs/runbooks/UNKNOWN-MESSAGES.md") {
		t.Error("the UNKNOWN alert must point to its runbook")
	}
}
