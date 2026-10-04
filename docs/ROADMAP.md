# Roadmap

Legenda: ✅ feito e testado · ⚠️ feito com limitação registrada · ⏭️ adiado (motivo/dependência).

## Primeira versão (escopo da especificação)

| Item | Estado | Notas |
|---|---|---|
| Boundaries hexagonais + teste de arquitetura (INV-06) | ✅ | `internal/archtest` |
| Entidades e state machines (instância, node, mensagem, migração, operação) | ✅ | testes unitários |
| Ports (provider, queue, bus, repos, blob, idempotency, lock) | ✅ | `internal/ports` |
| PostgreSQL + migrations + constraints de invariantes | ✅ | `migrations/0001_init.sql`; `postgres_integration_test.go` |
| Placement atômico, sticky, drain | ✅ | contrato com concorrência real (PostgreSQL) |
| Ownership, `assignment_epoch`, fencing lógico e físico, `MIGRATION_BLOCKED` | ✅ | `app/migration.go`, `Reassign` valida `OLD_OWNER_FENCED` na transação |
| Adapter Evolution v2 (client, auth, DTOs, mapping, webhook, capabilities, erros, compat) | ⚠️ | ver "Riscos conhecidos" |
| Redis Streams: CommandQueue particionada + EventBus | ✅ | contrato executado contra Redis real |
| Worker outbound (CAS, ordering, retry/backoff, DLQ, UNKNOWN) | ✅ | |
| Idempotência (create/send/delete/migrate) | ✅ | |
| Rate limit configurável com hierarquia | ⚠️ | estado por worker (ver abaixo) |
| Webhook inbound (auth por node, ownership, normalização, aceite durável: dedupe + outbox em uma transação) | ✅ | |
| Reconciler (drift, adoção, retomada, probe de nodes, dispatcher do outbox, janitor) | ✅ | |
| Outbox transacional + `sequence_no` + barreira `UNKNOWN` (INV-07 de ponta a ponta) | ✅ | ver ARCHITECTURE §6 |
| Serialização de lifecycle (`instance-control`) | ✅ | `TestLifecycle_*` |
| BlobStore S3-compatível, Claim-Check, validações, TTL, cleanup de órfãos | ✅ | RustFS (padrão) e SeaweedFS reais nos testes e no CI; MinIO só opt-in (imagem não baixável) |
| Observabilidade: métricas, logs estruturados, tracing OTel | ✅ | |
| SDK Python (instances, messages, operations, media) | ✅ | 14 testes + smoke contra a stack |
| Docker Compose (gateway, worker, reconciler, postgres, redis, rustfs|seaweedfs|minio por perfil, evolution-node-01/02) | ✅ | executado de ponta a ponta com Evolution v2.3.7 real |
| Testes de concorrência/falha da especificação | ✅ | [FAILURE-MODES](FAILURE-MODES.md); `-race` limpo (container) |
| OpenAPI, runbook, docs de arquitetura/adapters | ✅ | |

## Segundo review (recuperação e semântica distribuída)

| Item | Estado |
|---|---|
| Outbox recupera `DISPATCHING`; purge preserva estado de recuperação | ✅ |
| Epoch de origem nos eventos canônicos; projector rejeita evento stale | ✅ |
| Estados terminais de operação imutáveis | ✅ |
| Orçamento de retry durável (PostgreSQL) | ✅ |
| Retenção do EventBus configurável + lag/trim/perda + alertas | ✅ (definir o SLA de retenção do seu volume; Kafka como evolução) |
| `UNKNOWN` estrito por padrão | ✅ (`UNKNOWN_BARRIER_TIMEOUT` positivo = escolha explícita de disponibilidade) |
| `AWAITING_PAIRING` na migração | ✅ |
| `LookupInstance` (identidade do provider) | ✅ |
| Upload: `MaxBytesReader`, `Put` estrito, teste em object stores reais | ✅ |
| Release da imagem Evolution (scan/push/digest) | ✅ workflow `evolution-image` (build → confere Baileys → trivy CRITICAL → push GHCR → digest no summary); falta rodá-lo uma vez e fixar o digest no compose/manifests |
| CI (GitHub Actions) | ✅ `ci.yml`: gates, race, integração em 3 object stores, chaos, carga, carga multi-processo, SDK; actions fixadas por SHA (testado em `archtest`) |
| Chaos testing em infra real (`make test-chaos`): wipe e partição do Redis, stall do Postgres, workers mortos no meio do tráfego | ✅ achou e corrigiu um bug (consumer group perdido após wipe do Redis) |
| Jitter de rede e resets de conexão (proxy TCP entre a aplicação e PostgreSQL/Redis) | ✅ `TestJitter_*` |
| Baseline de carga (`make test-load`) | ✅ ver OPERATIONS; sem provider real a latência do WhatsApp não está incluída |
| Queda do object store (upload falha limpo; mensagem de mídia espera e sai uma vez) | ✅ `TestChaos_ObjectStoreOutage` |
| Carga multi-processo (gateway + workers + reconciler em containers, kill -9 de workers) com verificação do lado do provider | ✅ `make test-load-stack` |
| Carga com provider real (WhatsApp) | ⏭️ depende de números reais |

## Eventos para o tenant (requisito do conversation_agent)

| Item | Estado |
|---|---|
| Subscriptions por tenant (API, HMAC, rotação com grace, limite por tenant) | ✅ |
| Entrega at-least-once: retry com backoff, DLQ, redelivery, circuit breaker por destino | ✅ |
| Anti-SSRF no momento da conexão, sem redirects, resposta limitada | ✅ |
| `reply_to_provider_message_id` e timestamp do provedor no `message.received` | ✅ |
| `message.outbound_status` durável (outbox transacional) com `accepted_at` | ✅ |
| `IDEMPOTENCY_TTL` configurável (padrão 24 h) | ✅ |
| `GET /messages/{id}` com `provider_message_id`, `accepted_at`, `error_message` (R01) | ✅ |
| Criação idempotente de subscription (`Idempotency-Key`) (R04) | ✅ |
| `traceparent` nos webhooks; eventos de status carregam o trace do envio (R05) | ✅ |
| `GET /api/v1/limits` (janela de idempotência, tamanhos, ritmo, retries de webhook) (R13) | ✅ |
| `exclude_groups` na subscription (R14) | ✅ |
| Simulador/sandbox drivável do provedor, com exemplo e job de CI (R03) | ✅ [`SANDBOX.md`](SANDBOX.md) |
| Sequência inbound confiável (`source_sequence`) | ⏭️ o provedor não expõe; o consumidor ordena pelo `timestamp` do evento |
| Entrega estritamente ordenada por instância | ⏭️ melhor esforço (um em voo por assinatura+instância); estrita exigiria bloquear a fila atrás de uma entrega em retry |

## Limitações e riscos conhecidos (decisões conscientes)

0. **CVE-2026-48063 (Baileys):** mitigado com imagem derivada (Baileys `7.0.0-rc13`), validada com a stack real (create, QR, webhooks, delete). Pendente de homologação: pareamento com número real e, a médio prazo, migrar para uma release oficial da Evolution que já traga Baileys ≥ rc12 (as tags `2.4.0-rc2`/`latest` ainda não trazem; `homolog` não migra o banco).
1. **Evolution sem conta WhatsApp real.** Validado com a imagem real: criação, QR, estado, delete, probe, webhooks `qrcode.updated` e
   `connection.update`. *Não* validado: envio efetivo e payloads reais de `messages.upsert/update` (seguem a documentação e fakes).
   Dependência: um número de teste. Ação: rodar o fluxo completo e ajustar `webhook.go` se algum campo divergir.
2. **Migração entre nodes Evolution exige novo QR** (sessão local ao node; fencing via `logout`). Dependência: armazenamento de sessão
   compartilhável ou export/import de credenciais suportado pela Evolution.
3. **Rate limit é herança de política, não cota agregada, e é local ao worker.** Como uma partição tem um único consumidor, o limite por instância é exato na prática; ao mover a
   partição o histórico recomeça. Limites agregados (tenant/global somados entre workers) ⏭️: exige contador distribuído (Redis) —
   a interface `ratelimit.Limiter.Reserve` já comporta a troca.
4. **Mensagens aceitas antes de uma migração viram `STALE_COMMAND`** (conforme a especificação). Reenvio é responsabilidade do cliente.
5. **Barreira `UNKNOWN` estrita por padrão:** uma mensagem ambígua para a instância até `resolve`; `UNKNOWN_BARRIER_TIMEOUT=15m` (por exemplo) troca ordem estrita por disponibilidade.
6. **Chave de idempotência bloqueada por até `StaleAfter` (10 s)** após erro transitório não-rejeitado (ex.: falha de banco); retry
   com a mesma chave devolve `409 request_in_progress` nesse intervalo.
7. **Locks Redis de instância única** reduzem trabalho duplicado; a corretude nunca depende deles (CAS no PostgreSQL).

## Adiado

| Item | Motivo / dependência |
|---|---|
| Adapter `evolution/v3`, blue/green de providers | Não existe v3 pinável; o desenho de versionamento já suporta (PROVIDER-ADAPTERS) |
| Failover automático | Exige fencing verificável do owner antigo (não existe na Evolution sem destruir sessão); explicitamente fora da v1 |
| Adapters RabbitMQ / Kafka | Contratos prontos (`CommandQueue`/`EventBus`) e suítes de contrato reutilizáveis; implementar quando houver necessidade |
| Adapter filesystem de blobs | Somente dev/testes; o adapter em memória cobre os testes |
| Consumo de eventos pelo SDK (`consumer`) | O DoD termina em "publicar evento canônico"; falta definir o canal de entrega às aplicações (stream ACL por tenant, webhook de saída ou SSE) |
| RBAC além de `tenant`/`admin` | `Principal.Role` é o ponto de extensão |
| Dashboards Grafana | Métricas e regras de alerta de referência existem (`deploy/prometheus/alerts.yml`) |
| Helm chart / HPA | Há manifestos base em `deploy/kubernetes/` |
| Testes de carga/capacidade | Sem ambiente dedicado |
| Rotação de `WEBHOOK_SECRET` | Hoje exige recriar o webhook das instâncias (token derivado do secret) |
| Escala do Reconciler por sharding | Uma passada percorre o lote por `ListDue`; réplicas já são seguras |
