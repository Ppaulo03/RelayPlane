// Package archtest enforces the hexagonal boundaries (INV-06).
package archtest

import (
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const module = "github.com/relayplane/relayplane"

// infrastructure import path fragments that only adapters may use.
var infra = []string{
	"redis", "pgx", "lib/pq", "minio", "aws", "kafka", "rabbitmq", "amqp",
	"evolution", "baileys", "database/sql", "prometheus", "opentelemetry",
}

func repoRoot(t *testing.T) string {
	t.Helper()
	wd, _ := os.Getwd()
	for dir := wd; ; dir = filepath.Dir(dir) {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		if dir == filepath.Dir(dir) {
			t.Fatal("go.mod not found")
		}
	}
}

// importsOf returns import path -> file for all non-test Go files under dir.
func importsOf(t *testing.T, dir string) map[string][]string {
	t.Helper()
	out := map[string][]string{}
	err := filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(p, ".go") || strings.HasSuffix(p, "_test.go") {
			return err
		}
		f, perr := parser.ParseFile(token.NewFileSet(), p, nil, parser.ImportsOnly)
		if perr != nil {
			return perr
		}
		for _, imp := range f.Imports {
			path := strings.Trim(imp.Path.Value, `"`)
			out[path] = append(out[path], p)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// INV-06: the core imports neither adapters nor infrastructure SDKs, nor the
// application/transport layers; it may only use the standard library and
// itself.
func TestINV06_CoreDoesNotImportAdaptersOrInfrastructure(t *testing.T) {
	core := filepath.Join(repoRoot(t), "internal", "core")
	for path, files := range importsOf(t, core) {
		if !strings.Contains(path, ".") {
			continue // standard library
		}
		if !strings.HasPrefix(path, module+"/internal/core/") {
			t.Errorf("core must be self-contained, but %v import %q", files, path)
		}
		for _, bad := range infra {
			if strings.Contains(path, bad) {
				t.Errorf("core imports infrastructure %q in %v", path, files)
			}
		}
	}
}

// Ports depend on the core only.
func TestPortsDependOnlyOnCore(t *testing.T) {
	ports := filepath.Join(repoRoot(t), "internal", "ports")
	for path, files := range importsOf(t, ports) {
		if !strings.Contains(path, ".") {
			continue
		}
		if !strings.HasPrefix(path, module+"/internal/core/") {
			t.Errorf("ports may only import core, but %v import %q", files, path)
		}
	}
}

// Provider-specific code stays inside its adapter package.
func TestEvolutionTypesDoNotLeak(t *testing.T) {
	root := repoRoot(t)
	allowed := filepath.Join(root, "internal", "adapters", "providers", "evolution")
	err := filepath.WalkDir(filepath.Join(root, "internal"), func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(p, ".go") || strings.HasSuffix(p, "_test.go") {
			return err
		}
		if strings.HasPrefix(p, allowed) || strings.Contains(p, "archtest") {
			return nil
		}
		f, perr := parser.ParseFile(token.NewFileSet(), p, nil, parser.ImportsOnly)
		if perr != nil {
			return perr
		}
		for _, imp := range f.Imports {
			path := strings.Trim(imp.Path.Value, `"`)
			if strings.Contains(path, "adapters/providers/evolution") && !strings.Contains(p, filepath.Join("cmd")) &&
				!strings.Contains(p, filepath.Join("internal", "bootstrap")) {
				t.Errorf("%s imports the Evolution adapter; only the composition root may", p)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// Application services, workers and the API depend on ports, never on adapters
// (the composition root in internal/bootstrap and cmd/ wires them together).
func TestApplicationLayersDoNotImportAdapters(t *testing.T) {
	root := repoRoot(t)
	for _, layer := range []string{"app", "worker", "reconciler", "ratelimit", "idempotency", "api"} {
		dir := filepath.Join(root, "internal", layer)
		if _, err := os.Stat(dir); err != nil {
			continue
		}
		for path, files := range importsOf(t, dir) {
			if strings.Contains(path, module+"/internal/adapters") {
				t.Errorf("layer %s imports adapter %q in %v", layer, path, files)
			}
		}
	}
}
