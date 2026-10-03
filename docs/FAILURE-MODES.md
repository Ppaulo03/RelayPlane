# Modos de falha

Princípio: na primeira versão, **consistência > disponibilidade**. Cada linha abaixo tem teste (nome entre parênteses,
pacote `internal/systemtest` salvo indicação).

| Falha | Comportamento | Teste |
|---|---|---|
| Gateway morre após criar a sessão no provider, antes de persistir | Instância fica `ALLOCATING/CREATING`; o Reconciler vê a sessão existente e a **adota** (não duplica); `Provision` trata `already exists` como adoção. | `TestFailure_GatewayDiesAfterProviderCreate` |
| Provider/node fora do ar durante o create | Create devolve `202 CREATING` com `operation_id` (RUNNING); o Reconciler retoma assim que o node volta. Falha *permanente* (auth, rejeição) ⇒ `FAILED`, assignment liberado, capacidade devolvida. | `TestFailure_ProvisioningResumedByReconciler`, `…PermanentCreateFailureReleasesCapacity` |
| Worker morre após enviar e antes do ACK | A re-entrega encontra `DISPATCHING` ⇒ `UNKNOWN` (`WORKER_CRASH`); **nunca reenvia às cegas**. Se o envio já fora gravado, o comando duplicado é ack-ado sem efeito. | `TestFailure_WorkerCrashAfterSendBeforeAck`, `…RedeliveryIsIdempotent` |
| Redis redelivery / consumidor morto | Entradas pendentes são reivindicadas por quem assume a partição; CAS no estado da mensagem impede duplo envio (16 workers recebendo o mesmo comando ⇒ 1 envio). | `CommandQueueContract/RedeliversUnackedAfterConsumerCrash`, `TestConcurrent_SameCommandDeliveredToManyWorkersSendsOnce` |
| Timeout do provider *após* possível envio | Classificado `AMBIGUOUS` ⇒ `UNKNOWN`, sem retry automático. | `TestFailure_ProviderTimeoutIsAmbiguousAndNotRetried`, `ProviderContractSuite/ErrorTranslation` |
| Provider 503/5xx, socket caído | `RETRYABLE` com backoff `0/5s/30s/2m` e, esgotado, DLQ + `FAILED RETRIES_EXHAUSTED`. Ordem preservada; outras instâncias não esperam. | `…ProviderUnavailableRetriesThenDLQ`, `…RetryThenSuccess`, `…SocketDownThenRecovers` |
| Webhook duplicado | Deduplicado (`inbound_duplicates_total`); concorrência de 16 duplicatas ⇒ 1 publicação. | `TestINV05_*` |
| Webhook atrasado | `instance.status_changed` anterior ao catálogo é ignorado; recibo `delivered` após `read` não regride a mensagem. | `TestProjector_LateAndDuplicateEvents` |
| Webhook reivindica node/epoch que não é o owner | `OWNERSHIP_VIOLATION`: rejeitado (409), métrica, log ERROR e evento `ownership.violation`; nada é publicado. Tokens são por node (um node não fala por outro). | `TestWebhook_OwnershipViolationAndAuth`, `evolution/v2.TestWebhookAuthentication` |
| Comando com epoch antigo | `STALE_COMMAND`; nunca chega ao provider. | `TestINV02_INV08_*` |
| Transação PostgreSQL com rollback | Nada persistido, capacidade não vaza; o retry com a mesma `Idempotency-Key` funciona após o claim obsoleto (`StaleAfter`, 10 s). | `TestFailure_DatabaseRollbackDuringPlacement` (memória), `RepositoryContract` (PostgreSQL) |
| Heartbeat do node perdido | Node `DEGRADED → OFFLINE`; **nenhuma** migração/failover; instâncias ficam indisponíveis e o node volta sozinho a `READY`. Validado com Evolution real (container parado/iniciado). | `TestINV10_*` |
| Socket da instância caiu com o node saudável | Detectado por instância (reconciler/evento); node continua `READY`; sends são retentados; o reconciler pede reconexão (`ActionConnect`). | `TestINV10_*`, `TestFailure_SocketDownThenRecovers` |
| Sessão deslogada/precisa de QR | Reconciler **não** insiste em conectar (`AWAITING_PAIRING/LOGGED_OUT`); `reconnect` responde `PAIRING_REQUIRED`. | `reconciliation.TestDecide` |
| Blob removido antes do consumo | `FAILED MEDIA_UNAVAILABLE`; nada é enviado. Checksum divergente ⇒ `MEDIA_INVALID`. | `TestFailure_BlobRemovedBeforeConsumption`, `…BlobChecksumMismatch` |
| Falha de fencing na migração | `MIGRATION_BLOCKED`; owner/epoch antigos mantidos; o repositório recusa `Reassign` fora de `OLD_OWNER_FENCED`; retomável. | `TestINV09_FencingFailureBlocksMigration` |
| Migração sem capacidade no alvo (owner antigo já fenced) | Operação fica em `OLD_OWNER_FENCED` (`TARGET_UNAVAILABLE`), re-tentando com novo alvo; a instância está *down* até haver capacidade (consequência inevitável de ter fenced). | `app/migration.go` (`assign`) |
| Reconciler reinicia / roda duas vezes | Ações idempotentes; leases + CAS; sem eventos duplicados. | `TestFailure_ReconcilerRestartIsIdempotent` |
| Crash entre gravar `QUEUED` e publicar | *Outbox sweeper* republica `QUEUED` antigas; comandos duplicados são inofensivos. Ordem *best effort* só nesse caso. | `app.MessageService.RepublishStale` |
| Perda de lease de partição no meio do handler | Pode haver entrega duplicada; os handlers são idempotentes por CAS (nunca envio duplo). | contrato da fila |

## O que *não* é garantido (por design ou limite conhecido)

* Mensagens aceitas sob um assignment são **descartadas como `STALE_COMMAND`** se a instância migrar antes do envio.
  O cliente deve reenviar (o `message_id` mostra o estado).
* `UNKNOWN` exige decisão humana/da aplicação.
* Rate limit global/tenant é aplicado por worker (ver ROADMAP).
* Migração entre nodes Evolution exige novo pareamento (sessão é local ao node).
