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
| Webhook inbound (auth por node, ownership, normalização, dedupe 2 fases) | ✅ | |
| Reconciler (drift, adoção, retomada, probe de nodes, dispatcher do outbox, janitor) | ✅ | |
| Outbox transacional + `sequence_no` + barreira `UNKNOWN` (INV-07 de ponta a ponta) | ✅ | ver ARCHITECTURE §6 |
| Serialização de lifecycle (`instance-control`) | ✅ | `TestLifecycle_*` |
| BlobStore S3/MinIO, Claim-Check, validações, TTL, cleanup de órfãos | ✅ | MinIO real nos testes |
| Observabilidade: métricas, logs estruturados, tracing OTel | ✅ | |
| SDK Python (instances, messages, operations, media) | ✅ | 14 testes + smoke contra a stack |
| Docker Compose (gateway, worker, reconciler, postgres, redis, minio, evolution-node-01/02) | ✅ | executado de ponta a ponta com Evolution v2.3.7 real |
| Testes de concorrência/falha da especificação | ✅ | [FAILURE-MODES](FAILURE-MODES.md); `-race` limpo (container) |
| OpenAPI, runbook, docs de arquitetura/adapters | ✅ | |

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
5. **Barreira `UNKNOWN` com timeout:** o padrão (15 min) prefere disponibilidade; `UNKNOWN_BARRIER_TIMEOUT=0` dá ordem estrita ao custo de exigir `resolve`.
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
