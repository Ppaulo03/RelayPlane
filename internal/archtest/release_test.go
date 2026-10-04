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

// Every image the release publishes is built ONCE, and the build that is scanned is the build that is published. That is only true when
// no workflow builds again for the push: the single place that publishes is the composite action, which scans an OCI archive and pushes
// those same bytes.
func TestReleaseWorkflowScansEveryImageBeforePushingIt(t *testing.T) {
	root := repoRoot(t)
	files, _ := filepath.Glob(filepath.Join(root, ".github", "workflows", "*.yml"))
	users := 0
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		s := string(b)
		if regexp.MustCompile(`(?m)^\s*push:\s*true`).MatchString(s) || strings.Contains(s, "load: true") {
			t.Errorf("%s: images are published only by .github/actions/build-scan-push (a second build for the push would publish something that was never scanned)", filepath.Base(f))
		}
		if strings.Contains(s, "./.github/actions/build-scan-push") {
			users++
		}
	}
	if users < 2 {
		t.Errorf("the release (gateway, worker, reconciler) and the Evolution workflow must both publish through the composite action, found %d users", users)
	}

	a := readRepoFile(t, ".github", "actions", "build-scan-push", "action.yml")
	build, scan, push := strings.Index(a, "type=oci,dest="), strings.Index(a, "trivy-action"), strings.Index(a, `"$crane" push`)
	if build < 0 || scan < 0 || push < 0 || !(build < scan && scan < push) {
		t.Errorf("the action must build to an archive, scan it, and only then push (build=%d scan=%d push=%d)", build, scan, push)
	}
	if !strings.Contains(a, "provenance:") || !strings.Contains(a, "sbom: true") {
		t.Error("published images carry build provenance and an SBOM")
	}
	if !strings.Contains(a, `"$published" != "$SCANNED"`) {
		t.Error("the action must check that the registry holds the digest that was scanned")
	}
	if !strings.Contains(a, "exit-code: \"1\"") || !strings.Contains(a, "input: ${{ runner.temp }}/oci") {
		t.Error("the scan must fail the job and must look at the archive that is pushed")
	}
	if !strings.Contains(readRepoFile(t, ".github", "workflows", "release.yml"), "./.github/workflows/evolution-image.yml") {
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

// The OpenAPI contract is the base of generated clients: a path (or a method of a path) written twice is silently resolved by most parsers
// by keeping the last one, so half of the contract disappears without a warning.
func TestOpenAPIHasNoDuplicateKeys(t *testing.T) {
	lines := strings.Split(strings.ReplaceAll(readRepoFile(t, "docs", "openapi.yaml"), "\r\n", "\n"), "\n")
	path := regexp.MustCompile(`^  (/[^\s:]*):\s*$`)
	method := regexp.MustCompile(`^    (get|put|post|delete|patch|head|options):`)
	paths := map[string]bool{}
	var cur string
	seen := map[string]bool{}
	for n, l := range lines {
		if m := path.FindStringSubmatch(l); m != nil {
			cur = m[1]
			seen = map[string]bool{}
			if paths[cur] {
				t.Errorf("line %d: path %s is defined twice", n+1, cur)
			}
			paths[cur] = true
			continue
		}
		if m := method.FindStringSubmatch(l); m != nil && cur != "" {
			if seen[m[1]] {
				t.Errorf("line %d: %s %s is defined twice", n+1, strings.ToUpper(m[1]), cur)
			}
			seen[m[1]] = true
		}
	}
	if len(paths) < 30 {
		t.Errorf("the parser found only %d paths: this check is not looking at the contract", len(paths))
	}
}
