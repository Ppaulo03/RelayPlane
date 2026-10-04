# Roadmap: o que o RelayPlane precisa para sustentar o conversation_agent

Este documento lista o que falta no RelayPlane para que o [`conversation_agent`](../../conversation_agent/docs/DESIGN.md) possa ser implementado e operado sem remendos
do lado do agente. Ele é derivado dos requisitos do agente (`RUNTIME_PROTOCOL.md` §7–§8, `DESIGN.md` §27–§28, §38, §40, `ROADMAP.md` fases 2 e 6) e do estado real do código.

Legenda de tamanho: **S** ≤ 1 dia · **M** 2–4 dias · **L** ≥ 1 semana. `🔑` = precisa de um número de WhatsApp real.

## 1. Critério de "suficiente"

O agente é implementado em fases; só a **Fase 6** (RelayPlane + mídia + proativo) toca o RelayPlane. Logo existem três portões:

| Portão | Quando precisa estar verde | Significa |
|---|---|---|
| **G1 — Contrato** | antes de começar a Fase 6 do agente | o agente consegue ser escrito e testado contra o RelayPlane (inclusive no CI) sem WhatsApp |
| **G2 — Realidade** | antes de qualquer usuário real conversar com o agente | o comportamento foi observado com número real e o ambiente de staging existe |
| **G3 — Operação** | antes de produção aberta | HA, backup, observabilidade, segurança e capacidade |

As Fases 1–5 do agente (núcleo, confiabilidade, confirmação, flows, compiler) não dependem do RelayPlane: **G1 pode ser construído em paralelo a elas.**

## 2. Já entregue (PR #2 e anteriores)

| Requisito do agente | Estado |
|---|---|
| Receber mensagens inbound por webhook assinado, at-least-once, com retry/DLQ e redelivery | ✅ subscriptions |
| `reply_to_provider_message_id` e timestamp do provedor em `message.received` | ✅ validado contra uma Evolution **real** (R20; [`SPIKE-FINDINGS`](./SPIKE-FINDINGS.md)) |
| `message.outbound_status` durável com `accepted_at` | ✅ outbox transacional |
| `IDEMPOTENCY_TTL` configurável (padrão 24 h) | ✅ |
| Anti-SSRF, rotação de segredo, limite por tenant, isolamento entre tenants | ✅ |
| SDK Python com `subscriptions` e `verify_request` | ✅ |
| `UNKNOWN` resolvível por API (`POST /messages/{id}/resolve`) | ✅ |

## 3. G1 — Contrato (sem WhatsApp)

✅ = entregue (R01, R02, R03, R04, R05, R13, R14, R15). Faltam R07–R12 e o R16 (parte de documentação) abaixo.

| ID | Item | Por quê (requisito do agente) | Tam. | Pronto quando |
|---|---|---|---|---|
| **R01** ✅ | `GET /messages/{id}` expõe `provider_message_id`, `accepted_at`, `error_message`. **Hoje devolve só id, status, to, type, attempts, sequence_no, error_code** | o agente reconcilia o prompt de confirmação (`RUNTIME_PROTOCOL` §7/§8) e liga o `reply_to` do inbound ao seu prompt pelo `provider_message_id`; sem isso só dá para saber via evento (que pode ser perdido) | S | campo no OpenAPI, no SDK e teste que compara com o evento `message.outbound_status` |
| **R02** ✅ | **Contrato de eventos versionado:** `schema_version` no envelope, JSON Schema de cada evento (`docs/events/*.json`), seção `webhooks` no OpenAPI, e teste de contrato que valida amostras reais contra os schemas (Go e SDK Python) | o compiler/versionamento do agente fixa o que consome; evolução do evento não pode quebrar em silêncio | M | mudança incompatível de schema falha o CI; `docs/EVENTS.md` com exemplos. **Feito:** `docs/events/events.schema.json` (v1, campos não declarados proibidos), exemplos por tipo, `webhooks` no OpenAPI 3.1, conformidade verificada contra o que o código emite, contra os fixtures reais e contra tudo que a suíte de sistema entrega; o SDK recusa versão desconhecida. A métrica de latência (R12) segue pendente |
| **R03** ✅ | **Simulador/sandbox do provedor** (evolução do `cmd/loadstub`): API para injetar mensagens inbound (texto, `reply_to`, áudio, imagem, grupo), receipts, desconexão; perfil `sandbox` no compose | o CI do agente (Fase 6, chaos C07/C08/C13 e evals) precisa de um RelayPlane **real** ponta a ponta sem número de WhatsApp | M | `docker compose --profile sandbox up` + exemplo Python que envia, injeta resposta com `reply_to` e recebe o webhook |
| **R04** ✅ | **Criação idempotente de subscription** (`Idempotency-Key` ou nome único com upsert) | deploy do agente repetido hoje cria subscriptions duplicadas (limite 10) e entregas em dobro | S | reexecutar o bootstrap do agente não duplica |
| **R05** ✅ | **Propagação de trace:** `traceparent` nos webhooks e no `message.outbound_status` (a partir do trace do envio) | o trace do turno do agente (`DESIGN` §41) liga com o do RelayPlane | S | header presente; teste de ponta a ponta |
| **R06** ✅ | **Mídia inbound:** `message.received` com `media {type, mime, size, filename, duration}` e `media_id` baixável (claim-check: o RelayPlane baixa do provedor, grava no object store, expõe por `GET /media/{id}` com URL assinada), com limite de tamanho, TTL e tipo permitido | nota de voz → `Transcriber` do agente (`DESIGN` §27). **Hoje o payload só tem `type` e `text`: áudio chega sem referência alguma** | L | áudio de teste inbound vira `media_id` baixável; arquivo acima do limite é recusado com evento explicativo. **Feito** (desenho em [`EVENTS`](./EVENTS.md#anexos-payloadmedia)): o `message.received` com anexo só é entregue depois de resolvido (`READY`, `REJECTED` ou `FAILED`), baixado por `GET /api/v1/media/{id}/content`; fila durável idempotente, limites e TTL configuráveis, referência de download (com a chave) nunca exposta. **Validado com a Evolution real** (imagem, áudio OGG e PDF do spike baixados e decifrados por `getBase64FromMediaMessage` a partir da própria mensagem, sem estado no node); **falta** repetir com um número pareado ao vivo e com um vídeo grande (100 MB) para medir memória |
| **R07** | **Presença e leitura:** `POST /instances/{id}/presence {to, state: composing\|paused}` e `POST /messages/read` (marcar inbound como lido). A capacidade `Presence: true` já existe, mas **não há endpoint** | "pausas de digitação" do Compositor (`DESIGN` §25) e a UX de WhatsApp | M | ação chega ao provedor no simulador e no adapter Evolution |
| **R08** | **Resposta citada no envio:** `reply_to_message_id` no `POST /messages/send` | opcional: o prompt de confirmação pode citar a mensagem do usuário, e o usuário responde ao prompt | S–M | mensagem sai citando a original |
| **R09** | **Ciclo de vida das chaves de API:** várias chaves por tenant, rotação e revogação sem downtime, `last_used_at` (hoje: uma chave por tenant, sem rotação) | o agente é implantado e rotaciona credenciais sem parar | M | rotação com as duas chaves válidas durante a transição |
| **R10** | **Limites:** rate limit HTTP por tenant e backpressure por subscription (máx. em voo, pausar/retomar) | um agente lento ou em loop não pode degradar outros tenants nem inundar o dispatcher | M | tenant acima do limite recebe 429; subscription pausada acumula e retoma |
| **R11** | **Retenção e apagamento (LGPD):** retenção configurável de `outbound_messages.payload`, `event_outbox` e `webhook_deliveries` (**a DLQ guarda o texto do usuário para sempre**), e `DELETE /api/v1/contacts/{número}/data` por tenant | o agente (`DESIGN` §38) promete purge por contato; o RelayPlane guarda cópias do conteúdo | M–L | purge por contato apaga mensagens, eventos e entregas; teste verifica que nada sobra |
| **R12** | **Latência de entrega medida:** histograma `relayplane_event_delivery_lag_seconds` (evento → POST 2xx) e meta p95 < 2 s; ajustar o intervalo do outbox de eventos (hoje ~1 s) se necessário | o turno do agente espera o `ACCEPTED` do prompt antes de aceitar a confirmação; latência alta atrasa a conversa | S–M | métrica + teste de carga que fixa o p95 |
| **R13** ✅ | **`GET /api/v1/limits`:** devolve `idempotency_retention_seconds`, limites de texto/mídia, política de rate e limites de subscription | permite ao agente verificar `sender_retry_horizon <= retenção` na partida (contract test da Fase 6) | S | endpoint + uso no SDK |
| **R14** ✅ | **Filtro de grupos:** `exclude_groups` na subscription | grupos estão fora do escopo do agente (`DESIGN` §36); evita eventos inúteis e vazamento de conteúdo de grupo | S | subscription não recebe eventos de grupo |

### Achados do spike que viram itens novos

| ID | Item | Por quê | Tam. | Pronto quando |
|---|---|---|---|---|
| **R15** ✅ | **Número de sequência de entrega** no envelope, por (assinatura, instância), sem buracos | a Evolution reenvia webhooks em paralelo quando o gateway falha e o `messageTimestamp` tem resolução de 1 s: três mensagens do mesmo segundo chegaram como a, c, b e a ordem não é recuperável. A ordem em que as pessoas escreveram não é recuperável (empate de 1 s), mas a ordem **de entrega ao agente** e a perda de eventos são. **Decisão de desenho:** o número é por assinatura e instância (atribuído ao criar a entrega), não por instância global: com `exclude_groups` ou filtro de tipos, uma sequência global teria buracos legítimos e a detecção de perda deixaria de ser confiável | M | `sequence` no envelope, na API de entregas e no SDK (`Event`, `SequenceTracker`); contrato (memória e Postgres) garante 1,2,3 sem buracos, idempotente e estável na reentrega. **Feito** |
| **R16** | **Orientação ao agente sobre edição e exclusão:** edição chega como `type: secretEncrypted` sem texto; exclusão chega como `message.deleted` | um "sim" editado ou apagado depois de enviado não pode valer como confirmação (`INV-022`) | S | `docs/EVENTS.md` e exemplo no SDK; `message.deleted` ✅ já entregue |

## 4. G2 — Realidade (precisa de número 🔑 e de staging)

| ID | Item | Tam. | Pronto quando |
|---|---|---|---|
| **R20** ✅ | **Capturar payloads reais** da Evolution (texto, resposta citada, áudio, imagem, receipts, reconexão) como fixtures de ouro; **confirmar que `contextInfo.stanzaId` e `messageTimestamp` têm o significado assumido** e corrigir o adapter | S–M | fixtures no repositório; testes do adapter passam com payload real. **Feito:** 21 fixtures em `testdata/real`, `golden_test.go`; `stanzaId` e `messageTimestamp` confirmados; 4 defeitos corrigidos (mídia por URL interna, remetente LID, `message.deleted`, `date_time` local) |
| **R21** | **Imagens e deploy de staging:** rodar o workflow da imagem Evolution e fixar o digest; construir, escanear e publicar gateway/worker/reconciler no CI; ambiente de staging (compose ou Kubernetes) com a mesma configuração de produção | M | staging sobe a partir do CI; sem placeholders de digest |
| **R22** 🔑 (parcial) | **Roteiro de validação com número real**, automatizado onde possível: parear, enviar, receipt, resposta citada ida e volta, nota de voz, reconexão, queda do node e reposição **sem QR**, migração **com** QR; relatório com o que passou | M | relatório versionado em `docs/`. **Feito:** [`SPIKE-FINDINGS`](./SPIKE-FINDINGS.md) cobre parear, envio de texto e mídia, receipts, citação (direta e em grupo), nota de voz, node parado, gateway parado, edição, exclusão e logout. **Falta:** migração com QR, reação/enquete/localização, queda de conexão sem logout |
| **R23** | **Reposição de node:** runbook e garantia de fencing (um processo por sessão: StatefulSet de 1 réplica com o banco do node como único estado), validado no roteiro R22. A reposição com o mesmo banco reconecta sem QR (**verificado** em 2026-10-03: `connecting` → `open`, envio seguinte aceito); falta o runbook e a validação em staging | S–M | queda de node simulada em staging volta a `CONNECTED` sem QR |
| **R24** | **Runbook de `UNKNOWN`** para operadores e orientação ao agente (quando resolver como `sent`/`not_sent`) | S | documento + exemplo no SDK |

## 5. G3 — Operação

| ID | Item | Tam. |
|---|---|---|
| **R30** | PostgreSQL e Redis em HA, backups, PITR, simulado de restore; **os bancos dos nodes Evolution entram aqui, porque guardam as sessões** | L |
| **R31** | Kubernetes endurecido: requests/limits, PodDisruptionBudget, NetworkPolicy, Job de migração, testes de rolling upgrade e rollback | M |
| **R32** | Observabilidade real: Prometheus + Grafana, dashboards, simulado dos alertas, SLOs, retenção de logs sem PII | M |
| **R33** | Teste de capacidade com latência real do provedor; dimensionar `COMMAND_PARTITIONS` (a vazão é limitada por partições, não por workers) | M |
| **R34** | Segurança: rotação da chave admin, audit log, `govulncheck`/trivy nas imagens do app, SBOM e assinatura, revisão externa | M–L |
| **R35** | Compatibilidade em rolling upgrade (versões antiga e nova convivendo) e política de rollback de migration | M |

## 6. Ordem de execução e relação com as fases do agente

```
agent  Fases 1────────────5 │ Fase 6 ──────────────► piloto ───────────► produção
RelayPlane  [ G1 em paralelo ] ──────┘   [ G2 ]──────────┘   [ G3 ]
```

Pacotes de PR sugeridos para G1 (cada um independente e com testes):

1. **Contrato rápido:** R01, R04, R05, R13, R14 (quase tudo S; um PR).
2. **Contrato de eventos:** R02 ✅ + R15 ✅ + R12 (schemas, versão, métrica de latência).
3. **Sandbox:** R03 ✅ (desbloqueia o CI do agente; **é o item de maior alavanca**).
4. **Capacidades do canal:** R07, R08, depois **R06** (mídia inbound, o maior).
5. **Segurança e dados:** R09, R10, R11.

G2 começa quando houver um número de WhatsApp e um ambiente de staging; **R20 vem primeiro** porque, se o `reply_to` real for diferente do suposto, a confirmação do agente (`INV-022`) nasce errada.

## 7. Fora do escopo do RelayPlane (decisão do lado do agente)

* **Templates e janela de 24 h do WhatsApp:** a Evolution/Baileys é uma API não oficial e não tem esse conceito (`Templates: false`). A política de mensagens proativas é do agente, apoiada na rate policy do RelayPlane. Se o projeto migrar para a API oficial (Cloud API), vira um item novo.
* **Transcrição de áudio:** o RelayPlane só entrega a mídia (R06); o agente transcreve.
* **Sequência inbound do provedor (`source_sequence`):** o provedor não expõe, e o `timestamp` empata no mesmo segundo (verificado). A sequência de **aceitação** do RelayPlane é o item R15.
* **Entrega estritamente ordenada por instância:** continua melhor esforço (uma entrega em voo por assinatura e instância).

## 8. Riscos que este roadmap não elimina

* **API não oficial:** risco de bloqueio de número e de quebra a cada atualização do WhatsApp; o piloto deve usar números descartáveis primeiro.
* **Nodes singleton:** a queda de um node é indisponibilidade até a reposição (R23), não perda de sessão, **desde que** o banco do node seja preservado.
* **`UNKNOWN` exige decisão:** com `UNKNOWN_BARRIER_TIMEOUT=0` as mensagens seguintes esperam o `resolve`; o agente precisa de política para isso (R24).
