// Command render-release produces a deployable Kubernetes manifest from the repository template and the immutable image
// references of a release build:
//
//	render-release -in deploy/kubernetes/relayplane.yaml -out relayplane-v1.2.0.yaml \
//	    -gateway ghcr.io/o/relayplane-gateway@sha256:... -worker ... -reconciler ... -evolution ...
//
// It refuses to write a manifest that has a placeholder or an image that is not pinned by digest.
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/relayplane/relayplane/internal/release"
)

func main() {
	in := flag.String("in", "deploy/kubernetes/relayplane.yaml", "template manifest")
	out := flag.String("out", "", "rendered manifest to write (default: stdout)")
	images := map[string]*string{}
	for _, c := range release.Components {
		images[c] = flag.String(c, "", "immutable reference of the "+c+" image (repository@sha256:...)")
	}
	flag.Parse()
	raw, err := os.ReadFile(*in)
	if err != nil {
		fatal(err)
	}
	refs := map[string]string{}
	for c, v := range images {
		refs[c] = *v
	}
	rendered, err := release.Render(raw, refs)
	if err != nil {
		fatal(err)
	}
	if *out == "" {
		_, _ = os.Stdout.Write(rendered)
		return
	}
	if err := os.WriteFile(*out, rendered, 0o644); err != nil {
		fatal(err)
	}
	fmt.Fprintf(os.Stderr, "wrote %s (every image pinned by digest)\n", *out)
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "render-release:", err)
	os.Exit(1)
}
