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
| Redis perde os dados **depois** de aceitar o evento e de ele ser publicado, antes de qualquer consumidor lê-lo | Nada se perde: fan-out (`fanout_at`) e projector (`projected_at`) leem o `event_outbox` no banco. | `TestDurability_*`, `TestProjector_BrokerLossAfterPublishDoesNotLoseReceipt`, `EventFanOut`, `EventOutboxConsumers` |
| Redis/barramento fora, ou processo cai, **depois** de aceitar um webhook | O evento já está no banco (`event_outbox`): o provedor recebeu 200, o evento sai quando o barramento volta, uma vez. `relayplane_event_outbox_oldest_seconds` cresce e o alerta `RelayPlaneEventOutboxStalled` avisa. | `TestInbound_AcceptedEventSurvivesTheBrokerBeingDown`, `…IsDeliveredToTheTenantAfterAnOutage`, `Dedup` (contrato) |
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
| Crash/broker fora entre aceitar e publicar | Aceitar grava mensagem + `sequence_no` + comando no outbox **na mesma transação**; o dispatcher publica em ordem de sequência assim que o broker volta. Ordem preservada (A antes de B). | `TestOutbox_AcceptedButUnpublishedMessageIsNeverOvertaken`, `…DispatcherDrainsBacklogInOrder` |
| Broker perde um comando já publicado | O sucessor é devolvido ao outbox pela barreira de sequência (não ultrapassa); o dispatcher republica o perdido (despachado há > 2 min e ainda `QUEUED`) e depois o sucessor. | `TestOutbox_LostCommandIsRepublishedAndLaterMessagesWait` |
| Mensagem `UNKNOWN` (envio ambíguo) | **Barreira de ordem**: as seguintes da mesma instância esperam; outras instâncias seguem. Libera por `POST /messages/{id}/resolve` (padrão, ordem estrita) ou, se configurado, por `UNKNOWN_BARRIER_TIMEOUT`. | `TestBarrier_*` |
| Banco indisponível ao gravar `UNKNOWN`/`FAILED`/`ACCEPTED` | O comando **não é ACKado** (fica pendente, sem consumir tentativas); ao voltar, `DISPATCHING` vira `UNKNOWN` sem reenviar, ou o veredito `FAILED` pendente é gravado. | `TestDurability_*` |
| Comando `DISPATCHING` cujo comando sumiu do broker | O outbox republica (`DISPATCHING` parado além do limite); a re-entrega o transforma em `UNKNOWN` sem reenviar; com `UNKNOWN_BARRIER_TIMEOUT=0` os sucessores esperam o `resolve`. | `TestRecovery_DispatchingMessageWhoseCommandWasLostIsRecovered` |
| Broker fora durante a manutenção | O purge do outbox só roda após redispatch bem-sucedido e **nunca** apaga a entrada de mensagem `QUEUED`/`DISPATCHING`. | `TestRecovery_PurgeNeverDeletesEntriesOfRecoverableMessages` |
| Evento atrasado do owner anterior chega após a migração | Descartado como `STALE_ASSIGNMENT` (epoch de origem do evento ≠ epoch atual). | `TestProjector_StaleEventOfPreviousOwnerNeverAffectsTheNewAssignment` |
| Perda total do estado do broker (contador de tentativas volta a 1) | O orçamento de retry vem do `attempt_count` do banco: a mensagem ainda vai para DLQ na 5ª tentativa. | `TestRetryBudget_SurvivesBrokerStateLoss` |
| Dois drivers finalizam a mesma operação | O primeiro veredito vale; estado terminal é imutável (`ErrAlreadyTerminal`). | `RepositoryContract/Operations` |
| Consumidor do EventBus fora por mais tempo que a retenção | `events_lost`/`trim_risk` sobem (alertas em 50 %/90 % e em qualquer perda); eventos aparados não são recuperáveis do bus. | `TestEventBusStatsDetectTrimmingBeyondAConsumer` |
| Migração precisa de novo QR e o usuário demora | `AWAITING_PAIRING`, sem virar `FAILED`; conclui quando a sessão conecta; deletar cancela. | `TestMigration_AwaitingPairingIsNotAFailureAndNeverTimesOut` |
| Corpo de upload diferente do tamanho declarado (inclusive truncamento silencioso do MinIO) | `Put` estrito nos adapters + teto `MaxBytesReader` (413); nada fica armazenado. | `BlobStoreContract/DeclaredSizeIsExact`, `TestMediaUploadHardCap` |
| Redis perde os dados (restart sem persistência, failover para réplica vazia) | Streams, grupos, leases e contadores somem. A fila recria seus grupos ao receber `NOGROUP`; comandos recuperáveis são republicados e eventos continuam pelo outbox PostgreSQL. O que já fora enviado sem veredito vira `UNKNOWN` (nunca reenvia). | `TestChaos_RedisWipedWhileTrafficFlows`, `TestQueue_ConsumersRecoverAfterRedisLosesItsData`, `TestProjector_BrokerLossAfterPublishDoesNotLoseReceipt` |
| Redis ou PostgreSQL parados por alguns segundos (partição/stall) | Aceitação continua pelo outbox (Postgres) ou a escrita espera (Postgres parado); ao voltar, tudo converge, sem duplicata no provider e em ordem por instância. | `TestChaos_RedisPartition`, `TestChaos_PostgresStall` |
| Todos os workers morrem no meio do envio | Mensagens não confirmadas são re-entregues; as que estavam em voo no provider terminam `UNKNOWN` (resolvível) em vez de duplicar. | `TestChaos_WorkersKilledMidFlight` |
| Object store indisponível | Upload falha sem deixar blob `READY` sem bytes; mensagem de mídia já aceita espera (retry) e é entregue **uma vez** quando o store volta. O orçamento de retry precisa cobrir a duração da queda (`RetrySchedule`). | `TestChaos_ObjectStoreOutage` |
| Operações de lifecycle concorrentes (delete × migrate, reconnect × migrate, reconciler × migrate) | Lock `instance-control:<id>` serializa; o reconciler pula instância ocupada, nunca age durante migração ativa e relê o assignment antes de cada efeito colateral. | `TestLifecycle_*` |
| Perda de lease de partição no meio do handler | Pode haver entrega duplicada; os handlers são idempotentes por CAS (nunca envio duplo). | contrato da fila |

## O que *não* é garantido (por design ou limite conhecido)

* Mensagens aceitas sob um assignment são **descartadas como `STALE_COMMAND`** se a instância migrar antes do envio.
  O cliente deve reenviar (o `message_id` mostra o estado).
* `UNKNOWN` exige decisão humana/da aplicação (`resolve`) — ou o timeout da barreira, que troca ordem estrita por disponibilidade.
* O broker entrega *at-least-once* (comandos podem duplicar); o CAS no estado da mensagem garante um único envio.
* Rate limit global/tenant é aplicado por worker (ver ROADMAP).
* Migração entre nodes Evolution exige novo pareamento (sessão é local ao node).
