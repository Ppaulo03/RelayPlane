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

✅ = entregue: **todo o G1** (R01–R16), R23 e R24. Falta do G2 (R22 parcial e rodar o release de verdade) e o G3.

| ID | Item | Por quê (requisito do agente) | Tam. | Pronto quando |
|---|---|---|---|---|
| **R01** ✅ | `GET /messages/{id}` expõe `provider_message_id`, `accepted_at`, `error_message`. **Hoje devolve só id, status, to, type, attempts, sequence_no, error_code** | o agente reconcilia o prompt de confirmação (`RUNTIME_PROTOCOL` §7/§8) e liga o `reply_to` do inbound ao seu prompt pelo `provider_message_id`; sem isso só dá para saber via evento (que pode ser perdido) | S | campo no OpenAPI, no SDK e teste que compara com o evento `message.outbound_status` |
| **R02** ✅ | **Contrato de eventos versionado:** `schema_version` no envelope, JSON Schema de cada evento (`docs/events/*.json`), seção `webhooks` no OpenAPI, e teste de contrato que valida amostras reais contra os schemas (Go e SDK Python) | o compiler/versionamento do agente fixa o que consome; evolução do evento não pode quebrar em silêncio | M | mudança incompatível de schema falha o CI; `docs/EVENTS.md` com exemplos. **Feito:** `docs/events/events.schema.json` (v1, campos não declarados proibidos), exemplos por tipo, `webhooks` no OpenAPI 3.1, conformidade verificada contra o que o código emite, contra os fixtures reais e contra tudo que a suíte de sistema entrega; o SDK recusa versão desconhecida. A métrica de latência (R12) segue pendente |
| **R03** ✅ | **Simulador/sandbox do provedor** (evolução do `cmd/loadstub`): API para injetar mensagens inbound (texto, `reply_to`, áudio, imagem, grupo), receipts, desconexão; perfil `sandbox` no compose | o CI do agente (Fase 6, chaos C07/C08/C13 e evals) precisa de um RelayPlane **real** ponta a ponta sem número de WhatsApp | M | `docker compose --profile sandbox up` + exemplo Python que envia, injeta resposta com `reply_to` e recebe o webhook |
| **R04** ✅ | **Criação idempotente de subscription** (`Idempotency-Key` ou nome único com upsert) | deploy do agente repetido hoje cria subscriptions duplicadas (limite 10) e entregas em dobro | S | reexecutar o bootstrap do agente não duplica |
| **R05** ✅ | **Propagação de trace:** `traceparent` nos webhooks e no `message.outbound_status` (a partir do trace do envio) | o trace do turno do agente (`DESIGN` §41) liga com o do RelayPlane | S | header presente; teste de ponta a ponta |
| **R06** ✅ | **Mídia inbound:** `message.received` com `media {type, mime, size, filename, duration}` e `media_id` baixável (claim-check: o RelayPlane baixa do provedor, grava no object store, expõe por `GET /media/{id}` com URL assinada), com limite de tamanho, TTL e tipo permitido | nota de voz → `Transcriber` do agente (`DESIGN` §27). **Hoje o payload só tem `type` e `text`: áudio chega sem referência alguma** | L | áudio de teste inbound vira `media_id` baixável; arquivo acima do limite é recusado com evento explicativo. **Feito** (desenho em [`EVENTS`](./EVENTS.md#anexos-payloadmedia)): o `message.received` com anexo só é entregue depois de resolvido (`READY`, `REJECTED` ou `FAILED`), baixado por `GET /api/v1/media/{id}/content`; fila durável idempotente, limites e TTL configuráveis, referência de download (com a chave) nunca exposta. **Validado com a Evolution real** (imagem, áudio OGG e PDF do spike baixados e decifrados por `getBase64FromMediaMessage` a partir da própria mensagem, sem estado no node); **falta** repetir com um número pareado ao vivo e com um vídeo grande (100 MB) para medir memória |
| **R07** ✅ | **Presença e leitura:** `POST /instances/{id}/presence {to, state: composing\|paused}` e `POST /messages/read` (marcar inbound como lido). A capacidade `Presence: true` já existe, mas **não há endpoint** | "pausas de digitação" do Compositor (`DESIGN` §25) e a UX de WhatsApp | M | ação chega ao provedor no simulador e no adapter Evolution. **Feito:** `POST /instances/{id}/presence` (202, o node pausa sozinho, teto de 4 em curso) e `POST /messages/read`; o simulador impõe a validação real do node (`number`+`presence`+`delay`; `id`+`fromMe`+`remoteJid`). SDK 0.9 |
| **R08** ✅ | **Resposta citada no envio:** `reply_to_message_id` no `POST /messages/send` | opcional: o prompt de confirmação pode citar a mensagem do usuário, e o usuário responde ao prompt | S–M | mensagem sai citando a original. **Feito:** `payload.reply_to` por id do provedor (+ texto da prévia) ou por id de mensagem nossa. **Achado:** a Evolution, sem histórico, envia a resposta **sem citação e sem erro** quando recebe só o id; a prévia agora viaja na requisição e o simulador reproduz essa omissão silenciosa para o teste pegar a regressão. **Falta validar no aparelho** (a citação aparecer de fato) quando houver número pareado |
| **R09** ✅ | **Ciclo de vida das chaves de API:** várias chaves por tenant, rotação e revogação sem downtime, `last_used_at` (hoje: uma chave por tenant, sem rotação) | o agente é implantado e rotaciona credenciais sem parar | M | rotação com as duas chaves válidas durante a transição. **Feito:** `api_keys` (migração das chaves existentes), criar/listar/revogar, até 10 ativas, a última não se revoga, `last_used_at`, expiração opcional, rota de administrador para recuperar acesso, SDK 0.7 |
| **R10** ✅ | **Limites:** rate limit HTTP por tenant e backpressure por subscription (máx. em voo, pausar/retomar) | um agente lento ou em loop não pode degradar outros tenants nem inundar o dispatcher | M | tenant acima do limite recebe 429; subscription pausada acumula e retoma. **Feito:** balde por tenant (429 + `Retry-After`), pause/resume, teto de POSTs em voo por subscription, backlog visível. Achado no caminho: o repositório em memória ordenava entregas por horário (empata), agora por `sequence` e o contrato exige |
| **R11** ✅ | **Retenção e apagamento (LGPD):** retenção configurável de `outbound_messages.payload`, `event_outbox` e `webhook_deliveries` (**a DLQ guarda o texto do usuário para sempre**), e `DELETE /api/v1/contacts/{número}/data` por tenant | o agente (`DESIGN` §38) promete purge por contato; o RelayPlane guarda cópias do conteúdo | M–L | purge por contato apaga mensagens, eventos e entregas; teste verifica que nada sobra. **Feito:** retenção (mensagens 90 d, DLQ 30 d, configuráveis), `DELETE /contacts/{número}/data` (mensagens anonimizadas, eventos e DLQ apagados, arquivos removidos do object store), testado ponta a ponta nos backends reais. **Limites documentados:** streams do Redis, comando em voo, estado do provedor, logs e backups |
| **R12** ✅ | **Latência de entrega medida:** histograma `relayplane_event_delivery_lag_seconds` (evento → POST 2xx) e meta p95 < 2 s; ajustar o intervalo do outbox de eventos (hoje ~1 s) se necessário | o turno do agente espera o `ACCEPTED` do prompt antes de aceitar a confirmação; latência alta atrasa a conversa | S–M | métrica + teste de carga que fixa o p95. **Feito:** `relayplane_event_delivery_lag_seconds{event_type,attempt}`; `OUTBOX_INTERVAL` 1 s → 250 ms; a carga multiprocesso reporta p50/p95/p99 do lag e o CI falha acima de 2 s (medido: p95 ≈ 1,1 s com kill de workers e falha injetada) |
| **R13** ✅ | **`GET /api/v1/limits`:** devolve `idempotency_retention_seconds`, limites de texto/mídia, política de rate e limites de subscription | permite ao agente verificar `sender_retry_horizon <= retenção` na partida (contract test da Fase 6) | S | endpoint + uso no SDK |
| **R14** ✅ | **Filtro de grupos:** `exclude_groups` na subscription | grupos estão fora do escopo do agente (`DESIGN` §36); evita eventos inúteis e vazamento de conteúdo de grupo | S | subscription não recebe eventos de grupo |

### Achados do spike que viram itens novos

| ID | Item | Por quê | Tam. | Pronto quando |
|---|---|---|---|---|
| **R15** ✅ | **Número de sequência de entrega** no envelope, por (assinatura, instância), sem buracos | a Evolution reenvia webhooks em paralelo quando o gateway falha e o `messageTimestamp` tem resolução de 1 s: três mensagens do mesmo segundo chegaram como a, c, b e a ordem não é recuperável. A ordem em que as pessoas escreveram não é recuperável (empate de 1 s), mas a ordem **de entrega ao agente** e a perda de eventos são. **Decisão de desenho:** o número é por assinatura e instância (atribuído ao criar a entrega), não por instância global: com `exclude_groups` ou filtro de tipos, uma sequência global teria buracos legítimos e a detecção de perda deixaria de ser confiável | M | `sequence` no envelope, na API de entregas e no SDK (`Event`, `SequenceTracker`); contrato (memória e Postgres) garante 1,2,3 sem buracos, idempotente e estável na reentrega. **Feito** |
| **R16** ✅ | **Orientação ao agente sobre edição e exclusão:** edição chega como `type: secretEncrypted` sem texto; exclusão chega como `message.deleted` | um "sim" editado ou apagado depois de enviado não pode valer como confirmação (`INV-022`) | S | `docs/EVENTS.md` e exemplo no SDK; `message.deleted` ✅ já entregue. **Feito:** a receita da confirmação (citação + remetente + sem apagar/editar depois, em ordem de `sequence`) está em `docs/EVENTS.md` |

## 4. G2 — Realidade (precisa de número 🔑 e de staging)

| ID | Item | Tam. | Pronto quando |
|---|---|---|---|
| **R20** ✅ | **Capturar payloads reais** da Evolution (texto, resposta citada, áudio, imagem, receipts, reconexão) como fixtures de ouro; **confirmar que `contextInfo.stanzaId` e `messageTimestamp` têm o significado assumido** e corrigir o adapter | S–M | fixtures no repositório; testes do adapter passam com payload real. **Feito:** 21 fixtures em `testdata/real`, `golden_test.go`; `stanzaId` e `messageTimestamp` confirmados; 4 defeitos corrigidos (mídia por URL interna, remetente LID, `message.deleted`, `date_time` local) |
| **R21** ✅ (código) | **Imagens e deploy de staging:** rodar o workflow da imagem Evolution e fixar o digest; construir, escanear e publicar gateway/worker/reconciler no CI; ambiente de staging (compose ou Kubernetes) com a mesma configuração de produção | M | staging sobe a partir do CI; sem placeholders de digest. **Feito** ([`RELEASE`](./RELEASE.md)): workflow `release` (tag `v*`): build, **scan antes do push**, proveniência e SBOM das 4 imagens, `cmd/render-release` (recusa placeholder, tag e digest curto) e smoke das imagens publicadas; staging em compose (`deploy/staging`: produção, sem build, só digests) com um **smoke que roda em todo PR** (guardas de produção, nós Evolution `READY` na versão validada, QR do nó real). **Achado:** o scan do que seria o primeiro push da imagem Evolution teria falhado (4 CRITICAL corrigíveis: openssl, `tar` do npm, `fast-xml-parser` x3, `esbuild`); corrigidos no Dockerfile, e uma armadilha do `npm install --no-save` (que revertia o Baileys para a versão vulnerável) agora é barrada por uma verificação da árvore final. **Falta, e depende de você:** rodar o release de verdade (publica no seu GHCR), e um host/cluster permanente para o staging |
| **R22** 🔑 (parcial) | **Roteiro de validação com número real**, automatizado onde possível: parear, enviar, receipt, resposta citada ida e volta, nota de voz, reconexão, queda do node e reposição **sem QR**, migração **com** QR; relatório com o que passou | M | relatório versionado em `docs/`. **Feito:** [`SPIKE-FINDINGS`](./SPIKE-FINDINGS.md) cobre parear, envio de texto e mídia, receipts, citação (direta e em grupo), nota de voz, node parado, gateway parado, edição, exclusão e logout. **Falta:** migração com QR, reação/enquete/localização, queda de conexão sem logout |
| **R23** ✅ (parcial) | **Reposição de node:** [runbook](./runbooks/NODE-REPLACEMENT.md) e garantia de fencing: cada pod do StatefulSet usa **o seu próprio banco** (derivado do nome do pod; teste de arquitetura impede URI compartilhado). Reposição com o mesmo banco reconecta sem QR (**verificado** com número real em 2026-10-03). **Falta** repetir a queda/reposição num staging permanente com número | S–M | queda de node simulada em staging volta a `CONNECTED` sem QR |
| **R24** ✅ | **Runbook de `UNKNOWN`:** [runbook](./runbooks/UNKNOWN-MESSAGES.md), `GET /api/v1/messages?status=UNKNOWN`, `messages.list`, métricas `relayplane_unknown_messages`/`_oldest_seconds`, alerta e política de exemplo no SDK (`relayplane.unknown`: pergunta a um humano por padrão) | S | documento + exemplo no SDK |
| **R25** ✅ | **Aceite durável do inbound:** a chave de dedupe e o evento (ou o job do anexo) são gravados em **uma transação** antes do 200 ao provedor; o evento chega ao barramento pelo outbox. Fecha a janela em que um crash entre "visto" e "enfileirado" perdia a mensagem (o reenvio do provedor virava duplicata), e a perda do Redis depois do aceite | M | teste com o barramento fora e com o processo morto depois do aceite; o evento chega uma vez. **R26 ✅:** apagar um contato enquanto um evento ou anexo está em voo (marca de apagamento) |
| **R26** ✅ | **Apagamento resistente ao que está em voo:** o `DELETE /contacts/{número}/data` primeiro **marca** o contato (`contact_erasures`, hash do número) e depois apaga; o fan-out e o ingestor de anexos escrevem e **só depois** consultam a marca, removendo o que escreveram se o contato foi apagado depois do aceite da mensagem (`Event.AcceptedAt`, interno) | M | testes com o evento no barramento, no fan-out e com o anexo sendo baixado quando o pedido chega; uma mensagem posterior ao pedido é entregue |

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
4. **Capacidades do canal:** R07 ✅, R08 ✅, **R06** ✅ (mídia inbound, o maior).
5. **Segurança e dados:** R09 ✅, R10 ✅, R11 ✅.

G2 começa quando houver um número de WhatsApp e um ambiente de staging; **R20 vem primeiro** porque, se o `reply_to` real for diferente do suposto, a confirmação do agente (`INV-022`) nasce errada.

## 7. Fora do escopo do RelayPlane (decisão do lado do agente)

* **Templates e janela de 24 h do WhatsApp:** a Evolution/Baileys é uma API não oficial e não tem esse conceito (`Templates: false`). A política de mensagens proativas é do agente, apoiada na rate policy do RelayPlane. Se o projeto migrar para a API oficial (Cloud API), vira um item novo.
* **Transcrição de áudio:** o RelayPlane só entrega a mídia (R06); o agente transcreve.
* **Sequência inbound do provedor (`source_sequence`):** o provedor não expõe, e o `timestamp` empata no mesmo segundo (verificado). A sequência de **aceitação** do RelayPlane é o item R15.
* **Entrega estritamente ordenada por instância:** continua melhor esforço (uma entrega em voo por assinatura e instância).

## 8. Riscos que este roadmap não elimina

* **API não oficial:** risco de bloqueio de número e de quebra a cada atualização do WhatsApp; o piloto deve usar números descartáveis primeiro.
* **Nodes singleton:** a queda de um node é indisponibilidade até a reposição (R23), não perda de sessão, **desde que** o banco do node seja preservado.
* **`UNKNOWN` exige decisão:** com `UNKNOWN_BARRIER_TIMEOUT=0` as mensagens seguintes esperam o `resolve`; o agente tem uma política de exemplo para isso (R24, `relayplane.unknown`).
