// Command reconciler runs the control loop: desired/observed convergence,
// node probing, workflow resumption and housekeeping. Replicas are safe.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/relayplane/relayplane/internal/bootstrap"
	"github.com/relayplane/relayplane/internal/config"
	"github.com/relayplane/relayplane/internal/reconciler"
)

var version = "dev"

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "reconciler:", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	if err := cfg.Validate(true); err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	rt, err := bootstrap.New(ctx, cfg, "reconciler")
	if err != nil {
		return err
	}
	defer rt.Close(context.Background())
	if err := rt.SeedNodes(ctx); err != nil {
		return err
	}
	rt.ServeOps(ctx, ":"+cfg.OpsPort)
	if !cfg.ReconcilerEnabled {
		rt.Log.Warn("reconciler disabled by configuration")
		<-ctx.Done()
		return nil
	}
	rc := reconciler.DefaultConfig()
	rc.Interval, rc.InstanceInterval, rc.NodeOfflineAfter = cfg.ReconcilerInterval, cfg.InstanceInterval, cfg.NodeOfflineAfter
	rt.Log.Info("reconciler started", "version", version, "interval", rc.Interval.String())
	_ = reconciler.New(rt.App, rc, rt.Log).Run(ctx)
	return nil
}
