package ports

// Repositories groups the persistence ports for dependency injection.
type Repositories struct {
	Tenants     TenantRepository
	Instances   InstanceRepository
	Nodes       NodeRepository
	Operations  OperationRepository
	Messages    MessageRepository
	Blobs       BlobMetadataRepository
	Idempotency IdempotencyStore
	Dedup       Deduplicator
	// Events is the durable outbox of tenant-facing events produced by message status changes.
	Events        EventOutboxRepository
	Subscriptions SubscriptionRepository
	Deliveries    DeliveryRepository
}
