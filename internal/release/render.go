// Package release turns the Kubernetes manifest template of the repository into a deployable one: every placeholder image
// is replaced by an immutable reference (image@sha256:...) that a release build produced. A manifest that still has a
// placeholder, a floating tag or an image nobody built is never produced.
package release

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
)

// Components are the images the manifest refers to, by the name used in the template path
// (registry.example.com/relayplane/<component>:...).
var Components = []string{"gateway", "worker", "reconciler", "evolution"}

var (
	templateImage = regexp.MustCompile(`^(\s*(?:-\s+)?image:\s*)registry\.example\.com/relayplane/([a-z0-9-]+):\S*`)
	pinned        = regexp.MustCompile(`^[a-z0-9][a-z0-9.\-/_:]*@sha256:[0-9a-f]{64}$`)
	anyImage      = regexp.MustCompile(`^(?:-\s+)?image:\s*(\S+)`)
)

// Render replaces the template image of each component by images[component] and checks the result.
func Render(manifest []byte, images map[string]string) ([]byte, error) {
	for _, c := range Components {
		ref, ok := images[c]
		if !ok {
			return nil, fmt.Errorf("no image given for %q", c)
		}
		if !pinned.MatchString(ref) {
			return nil, fmt.Errorf("image for %q (%s) is not an immutable <repository>@sha256:<64 hex> reference", c, ref)
		}
	}
	for c := range images {
		if !contains(Components, c) {
			return nil, fmt.Errorf("unknown component %q (known: %s)", c, strings.Join(Components, ", "))
		}
	}
	used := map[string]bool{}
	lines := strings.Split(string(manifest), "\n")
	for i, line := range lines {
		m := templateImage.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		ref, ok := images[m[2]]
		if !ok {
			return nil, fmt.Errorf("line %d: the template refers to image %q, which is not a known component", i+1, m[2])
		}
		used[m[2]] = true
		lines[i] = m[1] + ref
	}
	var unused []string
	for _, c := range Components {
		if !used[c] {
			unused = append(unused, c)
		}
	}
	if len(unused) > 0 {
		return nil, fmt.Errorf("the template has no image line for %s", strings.Join(unused, ", "))
	}
	out := strings.Join(lines, "\n")
	if err := Verify([]byte(out)); err != nil {
		return nil, err
	}
	return []byte(out), nil
}

// Verify checks that a rendered manifest is fit to deploy: no placeholder is left and every image is pinned by digest.
// Comments are ignored (the template explains itself in them).
func Verify(manifest []byte) error {
	var problems []string
	for i, line := range strings.Split(string(manifest), "\n") {
		code := strings.TrimSpace(line)
		if j := strings.Index(code, " #"); j >= 0 { // inline comment
			code = strings.TrimSpace(code[:j])
		}
		if code == "" || strings.HasPrefix(code, "#") {
			continue
		}
		if strings.Contains(strings.ToUpper(code), "REPLACE_WITH") || strings.Contains(code, "registry.example.com") {
			problems = append(problems, fmt.Sprintf("line %d still holds a placeholder: %s", i+1, code))
		}
		if m := anyImage.FindStringSubmatch(code); m != nil {
			if ref := strings.Trim(m[1], `"'`); !pinned.MatchString(ref) {
				problems = append(problems, fmt.Sprintf("line %d: image %q is not pinned by digest", i+1, ref))
			}
		}
	}
	if len(problems) > 0 {
		sort.Strings(problems)
		return fmt.Errorf("manifest is not deployable:\n  %s", strings.Join(problems, "\n  "))
	}
	return nil
}

func contains(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}
