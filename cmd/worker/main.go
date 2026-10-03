// Command worker consumes outbound commands and projects canonical events.
// It scales horizontally: add replicas; partitions are shared through leases.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/relayplane/relayplane/internal/bootstrap"
	"github.com/relayplane/relayplane/internal/config"
	"github.com/relayplane/relayplane/internal/core/messaging"
	"github.com/relayplane/relayplane/internal/worker"
)

var version = "dev"

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "worker:", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	if err := cfg.Validate(false); err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	rt, err := bootstrap.New(ctx, cfg, "worker")
	if err != nil {
		return err
	}
	defer rt.Close(context.Background())
	rt.ServeOps(ctx, ":"+cfg.OpsPort)

	d := rt.App.Deps
	out := worker.NewOutbound(d.Repos, d.Providers, d.Blob, rt.Metrics, rt.Log)
	out.GlobalPolicy = messaging.RatePolicy{MinInterval: cfg.RateMinInterval, Burst: cfg.RateBurst, MaxPerMinute: cfg.RateMaxPerMinute,
		MaxConcurrent: cfg.RateMaxConcurrent, Cooldown: cfg.RateCooldown}
	out.MediaPolicy = d.Cfg.MediaPolicy
	proj := worker.NewProjector(d.Repos, rt.Log)

	rt.Log.Info("worker started", "version", version, "partitions", cfg.CommandPartitions)
	var wg sync.WaitGroup
	wg.Add(3)
	go func() { defer wg.Done(); _ = rt.Queue.Consume(ctx, out.Handle) }()
	go func() { defer wg.Done(); _ = rt.Bus.Subscribe(ctx, "projector", proj.Handle) }()
	go func() {
		defer wg.Done()
		t := time.NewTicker(10 * time.Second)
		defer t.Stop()
		for {
			if depth, err := rt.Queue.Depth(ctx); err == nil {
				rt.Metrics.OutboundQueueDepth.Set(float64(depth))
			}
			out.Limiter.Forget(10 * time.Minute)
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}
		}
	}()
	<-ctx.Done()
	rt.Log.Info("worker draining")
	wg.Wait()
	return nil
}
