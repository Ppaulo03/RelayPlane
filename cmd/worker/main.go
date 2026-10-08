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
	if err := cfg.Validate(); err != nil {
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
	out.UnknownBarrierTimeout = cfg.UnknownBarrierTimeout
	proj := worker.NewProjector(d.Repos, rt.Log)
	proj.Metrics = rt.Metrics

	rt.Log.Info("worker started", "version", version, "partitions", cfg.CommandPartitions)
	var wg sync.WaitGroup
	ingest := &worker.MediaIngestor{Repos: d.Repos, Providers: d.Providers, Blob: d.Blob, ErasureKey: d.Cfg.ErasureKey, Metrics: rt.Metrics, Log: rt.Log,
		MaxBytes: d.Cfg.EffectiveInboundMaxBytes(), TTL: d.Cfg.EffectiveInboundTTL(), Policy: d.Cfg.MediaPolicy}
	wg.Add(6)
	go func() { defer wg.Done(); ingest.Run(ctx) }()
	go func() { defer wg.Done(); _ = rt.Queue.Consume(ctx, out.Handle) }()
	go func() { defer wg.Done(); proj.Run(ctx) }()      // from the database, not from the broker
	go func() { defer wg.Done(); rt.FanOut.Run(ctx) }() // from the database, not from the broker
	go func() { defer wg.Done(); rt.Dispatcher.Run(ctx) }()
	go func() {
		defer wg.Done()
		t := time.NewTicker(10 * time.Second)
		defer t.Stop()
		for {
			if depth, err := rt.Queue.Depth(ctx); err == nil {
				rt.Metrics.OutboundQueueDepth.Set(float64(depth))
			}
			if c, err := d.Repos.InboundMedia.Counts(ctx, time.Now()); err == nil {
				rt.Metrics.InboundMediaPending.WithLabelValues("download").Set(float64(c.Download))
				rt.Metrics.InboundMediaPending.WithLabelValues("publish").Set(float64(c.Publish))
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
