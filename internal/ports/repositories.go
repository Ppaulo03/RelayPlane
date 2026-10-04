package ports

// Repositories groups the persistence ports for dependency injection.
type Repositories struct {
	Tenants     TenantRepository
	APIKeys     APIKeyRepository
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
	// InboundMedia is the durable work queue that resolves the attachments of inbound messages before they are delivered.
	InboundMedia InboundMediaRepository
}
