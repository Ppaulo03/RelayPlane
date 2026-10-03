package archtest

import (
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// Deployment artefacts are checked like code: a vulnerable provider build or a
// floating image tag must not be reintroduced by accident.

func deployFiles(t *testing.T) map[string]string {
	t.Helper()
	root := repoRoot(t)
	files := map[string]string{}
	add := func(p string) {
		if b, err := os.ReadFile(p); err == nil {
			rel, _ := filepath.Rel(root, p)
			files[filepath.ToSlash(rel)] = string(b)
		}
	}
	add(filepath.Join(root, "docker-compose.yml"))
	add(filepath.Join(root, "Dockerfile"))
	_ = filepath.WalkDir(filepath.Join(root, "deploy"), func(p string, d os.DirEntry, err error) error {
		if err == nil && !d.IsDir() {
			add(p)
		}
		return nil
	})
	return files
}

var baileysVersion = regexp.MustCompile(`BAILEYS_VERSION=([0-9A-Za-z.\-]+)`)

// CVE-2026-48063 (Baileys message spoofing) affects >= 7.0.0-rc.1 and < 7.0.0-rc12.
func TestEvolutionImageShipsAPatchedBaileys(t *testing.T) {
	files := deployFiles(t)
	df, ok := files["deploy/docker/evolution/Dockerfile"]
	if !ok {
		t.Fatal("deploy/docker/evolution/Dockerfile is missing: the Evolution image must be built with a patched Baileys")
	}
	m := baileysVersion.FindStringSubmatch(df)
	if m == nil {
		t.Fatal("Dockerfile must pin BAILEYS_VERSION")
	}
	rc := regexp.MustCompile(`^7\.0\.0-rc\.?(\d+)$`).FindStringSubmatch(m[1])
	if rc == nil {
		t.Fatalf("unexpected Baileys version %q (review the CVE range before changing the line)", m[1])
	}
	if n, _ := strconv.Atoi(rc[1]); n < 12 {
		t.Fatalf("Baileys %s is affected by CVE-2026-48063 (fixed in 7.0.0-rc12)", m[1])
	}
	if !strings.Contains(df, "@sha256:") {
		t.Error("the Evolution base image must be pinned by digest")
	}
}

func TestNoDeploymentUsesTheStockVulnerableEvolutionImageOrLatest(t *testing.T) {
	for name, content := range deployFiles(t) {
		if name == "deploy/docker/evolution/Dockerfile" {
			continue // the only place allowed to name the stock image: it is the base that gets patched
		}
		for i, line := range strings.Split(content, "\n") {
			trim := strings.TrimSpace(line)
			if strings.HasPrefix(trim, "#") {
				continue
			}
			if strings.Contains(trim, "image:") && strings.Contains(trim, "evoapicloud/evolution-api") {
				t.Errorf("%s:%d runs the stock Evolution image (Baileys 7.0.0-rc.9, CVE-2026-48063): use the image built from deploy/docker/evolution", name, i+1)
			}
			if regexp.MustCompile(`(image:|^FROM)\s+\S+:latest(\s|$|@)`).MatchString(trim) {
				t.Errorf("%s:%d uses a :latest tag", name, i+1)
			}
		}
	}
}

func TestExternalImagesArePinnedByDigest(t *testing.T) {
	for name, content := range deployFiles(t) {
		for i, line := range strings.Split(content, "\n") {
			trim := strings.TrimSpace(line)
			if i := strings.Index(trim, " #"); i >= 0 { // inline comment
				trim = strings.TrimSpace(trim[:i])
			}
			if strings.HasPrefix(trim, "#") || !strings.HasPrefix(trim, "image:") && !strings.HasPrefix(trim, "FROM ") {
				continue
			}
			ref := strings.Fields(trim)
			img := ref[len(ref)-1]
			if strings.HasPrefix(trim, "FROM ") && len(ref) > 2 { // FROM x AS y
				img = ref[1]
			}
			local := strings.HasPrefix(img, "relayplane/") || strings.HasPrefix(img, "registry.example.com/") || strings.Contains(img, "${")
			if !local && !strings.Contains(img, "@sha256:") {
				t.Errorf("%s:%d: image %q is not pinned by digest", name, i+1, img)
			}
		}
	}
}

var actionRef = regexp.MustCompile(`^\s*-?\s*uses:\s*(\S+)`)

// A mutable action tag (@v4) lets whoever controls that repository run code with this repository's token:
// every third-party action in a workflow must be pinned to a full commit SHA.
func TestWorkflowActionsArePinnedByCommitSHA(t *testing.T) {
	root := repoRoot(t)
	files, _ := filepath.Glob(filepath.Join(root, ".github", "workflows", "*.yml"))
	if len(files) == 0 {
		t.Skip("no workflows")
	}
	sha := regexp.MustCompile(`@[0-9a-f]{40}$`)
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		for i, line := range strings.Split(string(b), "\n") {
			m := actionRef.FindStringSubmatch(line)
			if m == nil || strings.HasPrefix(m[1], "./") {
				continue
			}
			if !sha.MatchString(m[1]) {
				t.Errorf("%s:%d: action %q is not pinned to a commit SHA", filepath.Base(f), i+1, m[1])
			}
		}
	}
}

// Anything that publishes an image must scan it first and print the digest to pin.
func TestImagePublishingWorkflowScansBeforePushing(t *testing.T) {
	b, err := os.ReadFile(filepath.Join(repoRoot(t), ".github", "workflows", "evolution-image.yml"))
	if err != nil {
		t.Skip("no image workflow")
	}
	s := string(b)
	scan, push := strings.Index(s, "trivy-action"), strings.Index(s, "push: true")
	if scan < 0 || push < 0 || scan > push {
		t.Error("the image must be scanned (trivy) BEFORE the step that pushes it")
	}
	if !strings.Contains(s, "7.0.0-rc13") {
		t.Error("the workflow must verify the patched Baileys version inside the built image")
	}
}
