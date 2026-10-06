// Command gateway is the RelayPlane control-plane API (REST + webhooks).
package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	apihttp "github.com/relayplane/relayplane/internal/api/http"
	"github.com/relayplane/relayplane/internal/bootstrap"
	"github.com/relayplane/relayplane/internal/config"
)

// version is stamped at build time (-ldflags "-X main.version=...").
var version = "dev"

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "gateway:", err)
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
	rt, err := bootstrap.New(ctx, cfg, "gateway")
	if err != nil {
		return err
	}
	defer rt.Close(context.Background())
	rt.ServeOps(ctx, ":"+cfg.OpsPort)
	if err := rt.SeedNodes(ctx); err != nil {
		return err
	}
	api := &apihttp.Server{App: rt.App, Metrics: rt.Metrics, HideMetrics: true, Log: rt.Log, Ready: rt.ReadyChecks(), MaxUpload: cfg.MediaMaxBytes,
		TenantRate: apihttp.RateConfig{PerSecond: cfg.APIRatePerSecond, Burst: cfg.APIRateBurst},
		Auth:       apihttp.KeyAuthenticator{Tenants: rt.App.Tenants, AdminKey: cfg.AdminAPIKey}}
	srv := &http.Server{Addr: ":" + cfg.HTTPPort, Handler: api.Handler(), ReadHeaderTimeout: 10 * time.Second, IdleTimeout: 120 * time.Second}
	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServe() }()
	rt.Log.Info("gateway listening", "addr", srv.Addr, "version", version)
	select {
	case err := <-errc:
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	case <-ctx.Done():
	}
	sctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	return srv.Shutdown(sctx)
}
