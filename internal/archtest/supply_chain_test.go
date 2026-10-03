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
