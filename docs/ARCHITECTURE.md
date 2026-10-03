# Arquitetura

## 1. Princípio

> Esta regra pertence ao domínio do RelayPlane ou é detalhe de Evolution, Redis, PostgreSQL ou S3?
> Detalhe de infraestrutura → adapter. Invariante do sistema → core.

A dependência aponta sempre para dentro: `adapters → ports ← core`. Isto é **verificado por teste**
(`internal/archtest`): o core só importa stdlib e a si mesmo; `ports` só importa o core; `app`, `worker`,
`reconciler`, `ratelimit`, `idempotency` e `api` não importam adapters; tipos Evolution não saem do pacote
`adapters/providers/evolution`. Apenas `internal/bootstrap` e `cmd/` conhecem adapters concretos.

```
cmd/{gateway,worker,reconciler} ─ bootstrap ─┬─ adapters/providers/evolution/v2   (MessagingProvider, WebhookAdapter)
                                             ├─ adapters/messaging/redisstreams   (CommandQueue, EventBus)
                                             ├─ adapters/persistence/postgres     (repositories, idempotency, dedup)
                                             ├─ adapters/blob/s3                  (BlobStore)
                                             └─ adapters/lock/redislock           (Locker)
        api/http ─▶ app ─▶ ports ◀─ adapters      worker ─▶ app/ports      reconciler ─▶ app/ports
                     └────▶ core (instance, ownership, routing, messaging, media, events, reconciliation)
```

## 2. Três loops

```
CONTROL  desired_state ─▶ Reconciler ─▶ MessagingProvider.GetInstanceState ─▶ observed_state
COMMAND  API ─▶ CommandQueue ─▶ Worker ─▶ MessagingProvider
EVENT    Provider ─▶ Webhook ─▶ auth/ownership ─▶ normalize ─▶ dedupe ─▶ EventBus ─▶ Projector
```

O estado pode mudar por **evento do provider** (`instance.status_changed`, milissegundos) ou por
**reconciliação periódica** (corrige eventos perdidos). Eventos nunca são fonte única de verdade.

## 3. Modelo

`Instance` (`internal/core/instance`): `id, tenant_id, provider, provider_instance_id, node_id,
assignment_epoch, desired_state, observed_state, last_provider_heartbeat, last_status_change` + timestamps.

* `desired_state`: `CONNECTED | DISCONNECTED | DELETED` (intenção).
* `observed_state`: `ALLOCATING → CREATING → AWAITING_PAIRING → CONNECTING → CONNECTED ⇄ DISCONNECTED/RECONNECTING/LOGGED_OUT`,
  `MIGRATING`, `DELETING`, `DELETED` (terminal), `FAILED`. Transições validadas em `instance.CanTransition`.
* `desired=CONNECTED, observed=DISCONNECTED` ⇒ *drift* ⇒ reconciliação.

`Provider Node` (`core/routing`): `STARTING | READY | DEGRADED | DRAINING | OFFLINE`. **Independente** da saúde da
instância: um node `READY` pode ter instâncias `DISCONNECTED` (INV-10).

## 4. Ownership, epoch e fencing

* `instance_assignments` é um histórico *append-oriented*; `UNIQUE (instance_id) WHERE released_at IS NULL`
  garante **no máximo um owner ativo no banco** (INV-01). Triggers forçam `epoch = anterior + 1`, impedem
  reabrir/reescrever histórico e impedir `assignment_epoch` de decrescer.
* **Logical fencing**: todo comando outbound carrega o `Assignment{instance, node, epoch}` do momento do aceite.
  O worker chama `ownership.ValidateDispatch`; qualquer diferença ⇒ `STALE_COMMAND`, mensagem `FAILED`,
  **o provider nunca é chamado** (INV-02/08).
* **Physical fencing**: o epoch não fecha um socket. A migração é uma state machine persistida
  (`operations.step`):

```
MIGRATION_REQUESTED → FENCING_OLD_OWNER ─(falha)→ MIGRATION_BLOCKED ─(novo POST /migrate)→ FENCING_OLD_OWNER
        └→ OLD_OWNER_FENCED → ASSIGN_NEW_EPOCH → START_NEW_OWNER → VERIFY_CONNECTION → CONNECTED
```

  `Provider.Disconnect` só retorna `nil` com o socket confirmado fechado (e é reverificado com
  `GetInstanceState`). **O banco recusa a troca de owner** se a operação não estiver em `OLD_OWNER_FENCED`
  (`InstanceRepository.Reassign` valida o `step` dentro da mesma transação) — INV-09 não depende só do código
  do serviço. Falhou a confirmação ⇒ `MIGRATION_BLOCKED` (consistência > disponibilidade).
  Quando o novo owner já existe e só falta o usuário escanear o QR, a operação fica **`AWAITING_PAIRING`** (status ativo, `PAIRING_REQUIRED`), sem
  timeout para `FAILED`: a infraestrutura está correta e o gargalo é o usuário. O timeout `MIGRATION_VERIFY_TIMEOUT` só vale para travas reais de
  verificação. Deletar a instância nesse estado cancela a migração (`CANCELLED_BY_DELETE`). Operações finalizadas (`SUCCEEDED`/`FAILED`) são imutáveis.
* **Sem failover agressivo**: node indisponível ⇒ suas instâncias ficam indisponíveis; heartbeat perdido nunca
  migra sessão. O Reconciler só marca o node `OFFLINE` (`routing.NextStatusAfterProbe`).

## 5. Placement e alocação atômica

`routing.Place` é uma função pura: só nodes `READY` com capacidade; menor utilização, desempate por id (determinístico).
É usada **apenas** na criação e em migração explícita (sticky). A reserva é atômica no adapter PostgreSQL:
`SELECT … FOR UPDATE` dos candidatos (ordem estável) + `UPDATE … WHERE active_instances < capacity` +
`CHECK (active_instances <= capacity)` na mesma transação que cria a instância e o assignment epoch 1.
Dois requests disputando o último slot: exatamente um vence (teste de contrato com 30 goroutines).

## 6. Ordering de ponta a ponta (INV-07)

A ordem por `instance_id` é garantida em **três camadas**, não só no broker:

1. **Transactional outbox + `sequence_no`.** Aceitar uma mensagem grava, na *mesma transação*, a linha em `outbound_messages`,
   o próximo `sequence_no` da instância (`instances.next_sequence`; o `UPDATE` segura o lock da linha até o commit, logo a ordem de
   sequência é a ordem de commit e não há buracos) e a linha do `outbox` com o comando já serializado. "Aceita" implica
   "será publicada": acabou a janela *INSERT → crash → PUBLISH*. O accept não depende do broker.
2. **Dispatcher do outbox** (`app.OutboxService`): publica as entradas pendentes de cada instância **estritamente em ordem de
   sequência**, sob um lock por instância (envio eager pelo gateway + loop de 1 s no reconciler). Crash entre publicar e marcar
   só duplica o comando (inofensivo: o worker faz CAS). Comandos que o broker perdeu (despachados há > 2 min e a mensagem segue
   `QUEUED`) são republicados.
3. **Barreira de sequência no worker**: a sequência *N* só é despachada quando toda sequência anterior da instância está
   resolvida (`ACCEPTED/DELIVERED/READ/FAILED`). Predecessor `QUEUED/DISPATCHING` ⇒ o comando volta ao outbox (ack + reset) e é
   republicado depois do predecessor (esperar no lugar travaria a chave se o predecessor reaparecer *atrás*). Predecessor
   **`UNKNOWN` é barreira**: não sabemos se saiu, então enviar *N* poderia inverter a conversa; as mensagens seguintes esperam
   (as de **outras** instâncias seguem) até `POST /messages/{id}/resolve` (`sent`/`not_sent`) ou até `UNKNOWN_BARRIER_TIMEOUT`
   (padrão `0` = indefinidamente: ordem estrita). Um timeout positivo é a escolha explícita "disponibilidade > ordem estrita" e é medido pelo
   relógio do banco. `FAILED` não bloqueia (provadamente não saiu).

## 7. Command queue (estratégia do adapter)

O **contrato** (`ports.CommandQueue`) exige: comandos com a mesma `PartitionKey` são entregues em ordem, um por vez;
chaves diferentes rodam em paralelo. A **estratégia** é do adapter:

*Redis Streams*: `N` streams fixos (`partition = fnv32a(instance_id) % N`, **não** um stream por instância).
Cada partição é servida por **um** consumidor por vez (lease com renovação por Lua); quem a detém processa
sequencialmente. Um comando que precisa esperar (backoff de retry, rate limit) bloqueia **só a sua chave**
(os demais da partição continuam; os seguintes da mesma chave ficam atrás). Entradas sem ACK permanecem
pendentes e são reivindicadas (`XAUTOCLAIM`) por quem assume a partição ⇒ *at-least-once*. Participação justa:
consumidores se registram num ZSET e liberam partições ociosas acima da sua cota. *Kafka* usaria `key = instance_id`.

`Disposition`: `Ack`, `Retry` (conta tentativa), `Defer` (não conta; usado pelo rate limit), `DeadLetter`.
Retry: `0, +5s, +30s, +2m, → DLQ` (`messaging.DefaultRetrySchedule`); nunca infinito.

## 8. Worker outbound

```
validar mensagem/estado → fencing lógico → (instância CONNECTED?) → rate limiter → claim QUEUED→DISPATCHING (CAS)
→ resolver mídia (valida tenant, tamanho, MIME, expiração, SHA-256) → provider.SendMessage → persistir → ACK
```

Estados: `QUEUED, DISPATCHING, ACCEPTED, DELIVERED, READ, FAILED, UNKNOWN`. Classificação de falha
(`errs.Classify`): `RETRYABLE` (provider provadamente não agiu), `AMBIGUOUS` (pode ter agido → `UNKNOWN`, **sem retry
automático**), `NON_RETRYABLE`. Um comando re-entregue que encontra a mensagem em `DISPATCHING` (worker morreu
após o claim) vira `UNKNOWN` — nunca reenvia às cegas.

## 9. Idempotência, dedupe, rate limit

* `Idempotency-Key` (por tenant): mesma chave+payload ⇒ mesmo resultado (replay, header `Idempotent-Replayed`);
  payload/operação diferente ⇒ `422 idempotency_key_reuse`; em andamento ⇒ `409`. Cada operação recebe o *resource id* já na
  reserva da chave; se o processo morre, o retry retoma com o mesmo id (create/send/delete/migrate são idempotentes por id).
* Dedupe inbound: chave `instance|event_type|provider_message_id|state` (sent/delivered/read **não** colapsam);
  protocolo em duas fases `Begin → publish → Commit` (falha de publish ⇒ `Abort` e o provider reenvia); `event_id`
  determinístico permite dedupe a jusante. Estado de conexão usa o timestamp do evento como id (CONNECTED pode ocorrer de novo).
* Rate limit (`core/messaging.RatePolicy`): `MinInterval, Burst, MaxPerMinute, MaxConcurrent, Cooldown`. A hierarquia
  `global < tenant < instance` é de **herança de política** (*merge* campo a campo em `ResolvePolicy`): o valor mais específico
  vence e cada instância é limitada **individualmente**. Isto **não** é uma cota agregada: "tenant X ≤ 100 msg/min somando todas as
  instâncias" exigiria limiters global/tenant em backend distribuído (ROADMAP). Nenhum valor fixo no código; o worker devolve
  `Defer(wait)` em vez de dormir. Estado local ao worker.

## 10. Mídia (Claim-Check) e blobs

Cliente → `POST /media/uploads` (reserva `<tenant>/media/<id>/<arquivo>`, valida tipo/tamanho/sha256; a reserva `PENDING` expira em
`MEDIA_PENDING_TTL`, 30 min) → `PUT …/content` (stream através do gateway, sem bufferizar; o tamanho **declarado é imposto durante o
stream** e o SHA-256 é verificado; `READY` passa a reter por `MEDIA_DEFAULT_TTL`) → `send` referencia `media_id`. Não há PUT
assinado direto ao bucket: ele não consegue limitar o tamanho efetivo (declarar 1 MB e enviar 10 GB). O envelope no broker carrega só `{object_key, content_type, size, sha256, expires_at}`.
Defesas: a API usa JSON estrito (campos desconhecidos, p.ex. `base64`, são rejeitados); `app.MessageService` e o adapter
da fila aplicam o limite inline (`MEDIA_INLINE_MAX_BYTES`) — INV-11; objetos têm TTL (`blob_metadata.expires_at`),
cleanup remove expirados e **órfãos**, e a lifecycle do bucket é rede de segurança; `CHECK` no banco impede chave fora
do namespace do tenant.

### Fencing de eventos, retry durável e identidade do provider
* **Eventos carregam `source_assignment {node_id, epoch}`**, fixado depois da validação do webhook; o projector aplica `instance.status_changed` sob esse epoch
  (`SetObserved(epoch do evento)`), logo um evento atrasado do owner anterior vira `STALE_ASSIGNMENT` e nunca altera a atribuição nova.
* **O orçamento de retry é o `attempt_count` do PostgreSQL** (`max(entrega do broker, attempt_count)`); uma perda total do Redis não reinicia a conta.
  Tentativas que falham antes do claim (instância não conectada) também são gravadas.
* **Adoção usa `LookupInstance`**: o core não assume que o id do provider é o id do RelayPlane.

## 11. Invariantes e onde são testadas

| Invariante | Testes principais |
|---|---|
| INV-01 um owner ativo | `contracttest.RepositoryContract/{OwnershipINV01,ReassignRequiresFencingINV09}`; `postgres.TestDatabaseEnforcesOwnershipInvariants`; `systemtest.TestINV01_*` |
| INV-02 / INV-08 epoch antigo nunca despachado | `ownership.TestValidateDispatch_StaleEpochRejected`; `systemtest.TestINV02_INV08_StaleCommandNeverReachesProvider` |
| INV-03 idempotência | `idempotency` (pacote); `systemtest.TestINV03_IdempotentOperations`; `TestConcurrent_Duplicate*` |
| INV-04 DRAINING não recebe | `routing.TestPlace_IneligibleNodesNeverChosen`; contrato `Placement/draining…`; `systemtest.TestINV04_*` |
| INV-05 eventos duplicados | contrato `Dedup`; `systemtest.TestINV05_*` (inclui 16 duplicatas concorrentes) |
| INV-06 core não importa adapters | `archtest.TestINV06_*` (+ ports, camadas, vazamento Evolution) |
| INV-07 ordem por instance_id | `CommandQueueContract/OrderingPerKeyUnderConcurrencyINV07` (memória e Redis); `RepositoryContract/OutboxAndSequence` (sequência sem buracos sob concorrência); `systemtest.TestINV07_*`, `TestOutbox_*`, `TestBarrier_*` |
| INV-09 fencing antes do novo owner | `ownership.TestCanActivateNewOwner_*`; contrato `ReassignRequiresFencing`; `systemtest.TestINV09_*` |
| INV-10 saúde node ≠ instância | `systemtest.TestINV10_*`; `ProviderContractSuite/SendWhileSocketDeadIsRetryable…` |
| INV-11 binário não passa pelo broker | `media.TestEnforceInlineLimit`; contrato da fila; `systemtest.TestINV11_*` |
| INV-12 convergência | `reconciliation.TestINV12_Converges`; `systemtest.TestINV12_ReconcilerConverges` |

A suíte de sistema roda em memória (rápida) **e** contra PostgreSQL+Redis+object store (RustFS, SeaweedFS e MinIO) reais
(`RELAYPLANE_SYSTEMTEST_BACKEND=real go test -tags integration ./internal/systemtest`).

## 12. API pública

`/api/v1/instances[...]`, `/messages`, `/operations`, `/media`, `/nodes` (admin), `/tenants` (admin), `/health/{live,ready}`,
`/metrics`, `/webhooks/{provider}`. A API **não menciona provider, node, epoch nem Baileys** (testado em
`api_test.TestPublicAPIDoesNotLeakProviderDetails`); erros são canônicos (`code` estável). Capability ausente ⇒
`501 capability_not_supported`. RBAC: `Principal{Role, TenantID}` — hoje `tenant` e `admin`.

## 13. Observabilidade

Métricas `relayplane_*` (lista completa da especificação + `outbound_messages_total`, `provider_request_seconds`,
`http_*`, `migration_blocked_total`); logs JSON com `trace_id, tenant_id, instance_id, message_id, node_id, provider,
assignment_epoch, operation_id` e redação de segredos/corpos; OpenTelemetry nos pontos: HTTP, DB (tracer do pgx),
publish/consume (propaga `traceparent` pelo broker), worker, chamada ao provider, blob, reconciliação
(exporter OTLP quando `OTEL_EXPORTER_OTLP_ENDPOINT` está definido).
