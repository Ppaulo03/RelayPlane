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

// A release is published WHOLE or not at all, and what is published is what was scanned. Phase 1 (build-scan) builds each image once to an
// OCI archive, scans it and keeps it; it has no permission to write packages. Phase 2 (publish) starts only when every scan passed and
// pushes those same bytes. Nothing else may push: a second build for the push would publish something nobody scanned.
func TestReleaseWorkflowScansEveryImageBeforePushingIt(t *testing.T) {
	root := repoRoot(t)
	files, _ := filepath.Glob(filepath.Join(root, ".github", "workflows", "*.yml"))
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		s := string(b)
		if regexp.MustCompile(`(?m)^\s*push:\s*true`).MatchString(s) || strings.Contains(s, "load: true") {
			t.Errorf("%s: images are published only by .github/actions/publish, from the archive that .github/actions/build-scan scanned (a second build for the push would publish something that was never scanned)", filepath.Base(f))
		}
	}

	// phase 1: build to an archive, scan, keep; never push
	b := readRepoFile(t, ".github", "actions", "build-scan", "action.yml")
	build, scan, keep := strings.Index(b, "type=oci,dest="), strings.Index(b, "trivy-action"), strings.Index(b, "upload-artifact")
	if build < 0 || scan < 0 || keep < 0 || !(build < scan && scan < keep) {
		t.Errorf("build-scan must build to an archive, scan it, and only then keep it (build=%d scan=%d keep=%d)", build, scan, keep)
	}
	if strings.Contains(b, "crane") || strings.Contains(b, "login") {
		t.Error("build-scan must not push or log in to a registry")
	}
	if !strings.Contains(b, "provenance:") || !strings.Contains(b, "sbom: true") {
		t.Error("published images carry build provenance and an SBOM")
	}
	if !strings.Contains(b, "exit-code: \"1\"") || !strings.Contains(b, "input: ${{ runner.temp }}/oci") {
		t.Error("the scan must fail the job and must look at the archive that is kept")
	}

	// phase 2: push exactly the kept archive and verify
	p := readRepoFile(t, ".github", "actions", "publish", "action.yml")
	for _, want := range []string{"download-artifact", `"$crane" push`, `"$published" != "$SCANNED"`, `"$digest" != "$scanned"`} {
		if !strings.Contains(p, want) {
			t.Errorf("publish lost %q: it must fetch the scanned archive, check it is the scanned digest, push it and check the registry holds that digest", want)
		}
	}

	// the release wires them: scan without write access to packages, publish only after the whole scan matrix
	r := readRepoFile(t, ".github", "workflows", "release.yml")
	scanJob := jobBlock(r, "scan")
	publishJob := jobBlock(r, "publish")
	if scanJob == "" || publishJob == "" {
		t.Fatal("the release needs a scan job and a publish job")
	}
	if strings.Contains(scanJob, "packages: write") {
		t.Error("the scan job must not be able to write packages: it cannot publish, even by mistake")
	}
	if !regexp.MustCompile(`(?m)^    needs:.*\bscan\b`).MatchString(publishJob) {
		t.Error("the publish job must need the scan job: it starts only when every image passed its scan")
	}
	if !strings.Contains(publishJob, "packages: write") {
		t.Error("the publish job is the one that writes packages")
	}
	for _, c := range []string{"gateway", "worker", "reconciler", "simulator", "evolution"} {
		if !strings.Contains(scanJob, "component: "+c) || !strings.Contains(publishJob, c) {
			t.Errorf("the component %q must be scanned and published by the same release", c)
		}
	}
	if !strings.Contains(scanJob, "./.github/actions/build-scan") || !strings.Contains(publishJob, "./.github/actions/publish") {
		t.Error("the release builds and scans with build-scan and publishes with publish")
	}
	if strings.Contains(r, "./.github/workflows/evolution-image.yml") {
		t.Error("the Evolution image is one more component of the release matrix: a separate publishing workflow would break 'whole or nothing'")
	}
}

// jobBlock returns the text of one top-level job of a workflow.
func jobBlock(workflow, name string) string {
	re := regexp.MustCompile(`(?m)^  ` + regexp.QuoteMeta(name) + `:\n`)
	loc := re.FindStringIndex(workflow)
	if loc == nil {
		return ""
	}
	rest := workflow[loc[1]:]
	if next := regexp.MustCompile(`(?m)^  [a-z][a-z-]*:\n`).FindStringIndex(rest); next != nil {
		rest = rest[:next[0]]
	}
	return rest
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

// The sandbox that runs WITHOUT the repository must really not need it: no build, no overlay on another compose file, every image from the
// release, and nothing published to the host beyond the loopback.
func TestStandaloneSandboxNeedsNothingFromTheRepository(t *testing.T) {
	c := readRepoFile(t, "deploy", "sandbox", "compose.yml")
	if regexp.MustCompile(`(?m)^\s+build:`).MatchString(c) || strings.Contains(c, "!override") || strings.Contains(c, "!reset") || strings.Contains(c, "extends:") {
		t.Error("the standalone sandbox must be self-contained: no build, no overlay, no extends")
	}
	for _, v := range []string{"GATEWAY_IMAGE", "WORKER_IMAGE", "RECONCILER_IMAGE", "SIMULATOR_IMAGE"} {
		if !strings.Contains(c, "${"+v+":?") {
			t.Errorf("the sandbox must take %s from the release (no default)", v)
		}
	}
	for _, m := range regexp.MustCompile(`(?m)^\s+ports:\s*\["([^"]+)"`).FindAllStringSubmatch(c, -1) {
		if !strings.HasPrefix(m[1], "127.0.0.1:") {
			t.Errorf("a sandbox port must be bound to the loopback: %s", m[1])
		}
	}
	if regexp.MustCompile(`(?m)^\s+\./`).MatchString(c) || strings.Contains(c, " - ./") {
		t.Error("the standalone sandbox mounts nothing from the repository")
	}
	r := readRepoFile(t, ".github", "workflows", "release.yml")
	for _, want := range []string{"component: simulator", "SIMULATOR_IMAGE=", "relayplane-sandbox-$VERSION.tar.gz", "deploy/sandbox/sandbox.sh", "deploy/sandbox/compose.yml"} {
		if !strings.Contains(r, want) {
			t.Errorf("the release must publish the sandbox bundle: missing %q", want)
		}
	}
}
