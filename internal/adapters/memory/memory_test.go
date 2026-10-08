package memory_test

import (
	"testing"

	"github.com/relayplane/relayplane/internal/adapters/memory"
	"github.com/relayplane/relayplane/internal/contracttest"
	"github.com/relayplane/relayplane/internal/core/instance"
	"github.com/relayplane/relayplane/internal/core/ownership"
	"github.com/relayplane/relayplane/internal/ports"
)

func TestRepositoryContract(t *testing.T) {
	contracttest.RepositoryContract(t, func(t *testing.T) ports.Repositories { return memory.NewStore().Repositories() })
}

func TestCommandQueueContract(t *testing.T) {
	contracttest.CommandQueueContract(t, func(t *testing.T) ports.CommandQueue {
		return memory.NewQueue(memory.QueueConfig{Partitions: 8, InlineMaxBytes: contracttest.QueueInlineLimit})
	})
}

func TestEventBusContract(t *testing.T) {
}

func TestBlobStoreContract(t *testing.T) {
	contracttest.BlobStoreContract(t, func(t *testing.T) ports.BlobStore { return memory.NewBlob() })
}

func TestLockerContract(t *testing.T) {
	contracttest.LockerContract(t, func(t *testing.T) ports.Locker { return memory.NewLocker() })
}

func TestFakeProviderPassesProviderContractSuite(t *testing.T) {
	contracttest.ProviderContractSuite(t, func(t *testing.T) contracttest.ProviderHarness {
		f := memory.NewFakeProvider()
		kinds := map[contracttest.Failure]memory.FailKind{
			contracttest.Unavailable: memory.FailUnavailable, contracttest.Ambiguous: memory.FailAmbiguous,
			contracttest.AuthFailed: memory.FailAuth, contracttest.NotFound: memory.FailNotFound,
		}
		return contracttest.ProviderHarness{
			Provider: f, NodeID: "node-01", RejectsStale: true,
			Inject: func(k contracttest.Failure) { f.FailNext(kinds[k]) },
			Pair:   func(a ownership.Assignment) { f.SetState(a.InstanceID, instance.Connected) },
			Drop:   func(a ownership.Assignment) { f.SetState(a.InstanceID, instance.Disconnected) },
		}
	})
}
