//go:build integration

package postgres_test

import (
	"context"
	"os"
	"testing"

	"github.com/relayplane/relayplane/internal/adapters/persistence/postgres"
	"github.com/relayplane/relayplane/internal/contracttest"
	"github.com/relayplane/relayplane/internal/ports"
)

func url(t *testing.T) string {
	u := os.Getenv("RELAYPLANE_TEST_DATABASE_URL")
	if u == "" {
		u = "postgres://relayplane:relayplane@127.0.0.1:55440/relayplane_test?sslmode=disable"
	}
	return u
}

func openStore(t *testing.T) *postgres.Store {
	t.Helper()
	ctx := context.Background()
	s, err := postgres.Open(ctx, url(t))
	if err != nil {
		t.Skipf("postgres unavailable: %v", err)
	}
	t.Cleanup(s.Close)
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if err := s.Reset(ctx); err != nil {
		t.Fatal(err)
	}
	return s
}

func TestRepositoryContract(t *testing.T) {
	contracttest.RepositoryContract(t, func(t *testing.T) ports.Repositories {
		return openStore(t).Repositories()
	})
}

func TestMigrateIsIdempotent(t *testing.T) {
	s := openStore(t)
	for i := 0; i < 3; i++ {
		if err := s.Migrate(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
}

// The database itself enforces the ownership invariants, independently of the Go code.
func TestDatabaseEnforcesOwnershipInvariants(t *testing.T) {
	s := openStore(t)
	ctx := context.Background()
	p := s.Pool()
	mustExec := func(sql string, args ...any) error { _, err := p.Exec(ctx, sql, args...); return err }

	if err := mustExec(`INSERT INTO tenants(id,name,api_key_hash) VALUES('t1','t','h')`); err != nil {
		t.Fatal(err)
	}
	for _, n := range []string{"n1", "n2"} {
		if err := mustExec(`INSERT INTO provider_nodes(id,provider,endpoint,capacity,status) VALUES($1,'p','http://x',2,'READY')`, n); err != nil {
			t.Fatal(err)
		}
	}
	if err := mustExec(`INSERT INTO instances(id,tenant_id,name,provider,node_id,assignment_epoch,desired_state,observed_state)
		VALUES('i1','t1','n','p','n1',1,'CONNECTED','ALLOCATING')`); err != nil {
		t.Fatal(err)
	}
	if err := mustExec(`INSERT INTO instance_assignments(instance_id,node_id,epoch) VALUES('i1','n1',1)`); err != nil {
		t.Fatal(err)
	}

	t.Run("INV-01 two open assignments are impossible", func(t *testing.T) {
		if err := mustExec(`INSERT INTO instance_assignments(instance_id,node_id,epoch) VALUES('i1','n2',2)`); err == nil {
			t.Fatal("second open assignment accepted")
		}
	})
	t.Run("epoch must be exactly previous+1", func(t *testing.T) {
		if err := mustExec(`UPDATE instance_assignments SET released_at=now(), release_reason='X' WHERE instance_id='i1'`); err != nil {
			t.Fatal(err)
		}
		if err := mustExec(`INSERT INTO instance_assignments(instance_id,node_id,epoch) VALUES('i1','n2',5)`); err == nil {
			t.Fatal("epoch gap accepted")
		}
		if err := mustExec(`INSERT INTO instance_assignments(instance_id,node_id,epoch) VALUES('i1','n2',1)`); err == nil {
			t.Fatal("epoch reuse accepted")
		}
		if err := mustExec(`INSERT INTO instance_assignments(instance_id,node_id,epoch) VALUES('i1','n2',2)`); err != nil {
			t.Fatalf("valid next epoch rejected: %v", err)
		}
	})
	t.Run("released history is immutable", func(t *testing.T) {
		if err := mustExec(`UPDATE instance_assignments SET release_reason='TAMPER' WHERE instance_id='i1' AND epoch=1`); err == nil {
			t.Fatal("history rewritten")
		}
		if err := mustExec(`UPDATE instance_assignments SET released_at=NULL, release_reason=NULL WHERE instance_id='i1' AND epoch=1`); err == nil {
			t.Fatal("released assignment reopened")
		}
	})
	t.Run("instance epoch never decreases", func(t *testing.T) {
		if err := mustExec(`UPDATE instances SET assignment_epoch=2 WHERE id='i1'`); err != nil {
			t.Fatal(err)
		}
		if err := mustExec(`UPDATE instances SET assignment_epoch=1 WHERE id='i1'`); err == nil {
			t.Fatal("epoch decreased")
		}
	})
	t.Run("capacity cannot be exceeded", func(t *testing.T) {
		if err := mustExec(`UPDATE provider_nodes SET active_instances=3 WHERE id='n1'`); err == nil {
			t.Fatal("overbooked node accepted")
		}
	})
	t.Run("single active migration per instance", func(t *testing.T) {
		q := `INSERT INTO operations(id,tenant_id,instance_id,type,status,step) VALUES($1,'t1','i1','MIGRATE','RUNNING','MIGRATION_REQUESTED')`
		if err := mustExec(q, "op1"); err != nil {
			t.Fatal(err)
		}
		if err := mustExec(q, "op2"); err == nil {
			t.Fatal("second active migration accepted")
		}
	})
	t.Run("blob keys stay in the tenant namespace", func(t *testing.T) {
		q := `INSERT INTO blob_metadata(id,tenant_id,object_key,content_type,size,sha256,status,expires_at) VALUES($1,'t1',$2,'x',1,'a','PENDING',now())`
		if err := mustExec(q, "b1", "t2/media/x"); err == nil {
			t.Fatal("cross-tenant object key accepted")
		}
		if err := mustExec(q, "b2", "t1/media/x"); err != nil {
			t.Fatal(err)
		}
	})
}
