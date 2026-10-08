# TODO

O que **falta** no RelayPlane, em ordem aproximada de prioridade. O que já foi entregue está em [`AGENT-READINESS`](./AGENT-READINESS.md) (requisitos R01 a R29) e o estado das garantias em [`CONTRACT`](./CONTRACT.md) (seção 6). Atualize este arquivo no mesmo PR que fecha um item.

Legenda: 🔑 precisa de número de WhatsApp real · 👤 decisão ou ação sua (não do código) · 🧱 só código/CI.

## 1. Antes de colocar conversas reais de clientes

- [ ] 🔑 **Validação no aparelho** do que hoje só foi provado com simulador e formatos capturados: mídia nos dois sentidos, resposta citada, "digitando", confirmação de leitura. Repetir a queda do node com o mesmo banco (reconecta sem QR) com a stack de hoje.
- [ ] 🔑 **Casos do roteiro R22 ainda não vistos:** migração de instância com QR, reação, enquete, localização, queda de conexão **sem** logout, vídeo grande (100 MB) para medir memória do ingestor.
- [ ] 🧱👤 **Um release candidato novo** com tudo o que entrou depois do `0.3.0-rc3` (entrega ao tenant pelo banco, apagamento fechado nas bordas, contrato `schema_version` 2, limites de assinatura), validado com número real antes do oficial.
- [ ] 👤 **Primeiro release oficial:** escolher o número (`0.3.0`?), alinhar `docs/VERSIONS.md` e o `docker-compose.yml` (ainda citam 0.2.0), criar a tag `v<versão>` (gera os artefatos e o release no GitHub). Os candidatos `0.3.0-rc1`, `-rc2` e `-rc3` já estão públicos no GHCR: não reutilize esses números.
- [ ] 👤 **Quem implanta** (decidido: não é do módulo): host, DNS, TLS, segredos, backup e restauração do Postgres e dos bancos dos nodes, e **bloquear `/metrics`** no ingress (está na porta pública do gateway).

## 2. Dívidas técnicas conhecidas

Durabilidade e consistência:
- [ ] 🧱 **Cota de backlog por assinatura só conta entregas** (`WEBHOOK_MAX_PENDING_PER_SUBSCRIPTION`); não limita bytes nem idade além da retenção de pendentes (30 dias). A DLQ também cresce até a retenção dela.
- [ ] 🧱 **`UNKNOWN` resolvido pelo próprio RelayPlane:** consultar o node (o Evolution expõe consulta de mensagem?) para saber se a mensagem existe e resolver sozinho, em vez de parar a instância até alguém decidir. Só vale se a consulta for confiável; validar com número real.

Privacidade (apagamento de contato):
- [ ] 👤 **Reenvio muito antigo depois do TTL do dedupe:** um webhook antigo reapresentado ganha `AcceptedAt = agora` e a marca o trata como dado novo. Usar também o carimbo do provedor fecha o caso, mas descartaria uma mensagem que a pessoa mandou *antes* do pedido e que chegou depois (celular offline). Escolha de política de privacidade.
- [ ] 🧱 O apagamento depende de **relógios sincronizados** entre os serviços (a marca compara a hora do aceite com a do pedido), **não recolhe um POST que já saiu para a rede** e **não alcança** o que já está gravado no Redis até ser consumido, logs, backups nem o estado do WhatsApp/Evolution.
- [ ] 🧱 **`ERASURE_KEY` precisa ficar estável:** trocá-la faz as marcas antigas deixarem de casar. As marcas escritas pelos builds `rc` até o `rc3` (hash simples, sem chave) ficam inertes. Se virar requisito rotacionar a chave, é preciso um esquema de chaves múltiplas.

Qualidade:
- [ ] 🧱 **Teste instável sob `-race`:** `TestEventDeliveryLag_RetriesAreSeparatedFromTheHealthyPath` falha ~2 em 120 execuções na `main` (a amostra de latência do retry cai no balde de 25 ms, e o teste espera pelo menos 250 ms de backoff). Na falha, os dois pedidos do consumidor aparecem com `At` fora de ordem (o de tentativa 2 *antes* do de tentativa 1); ainda não achei a causa (suspeita: carimbo `At` do `Receiver` de teste, ou relógio). Reproduzir com `go test -race -count=250 -run <teste> ./internal/systemtest` num container Go.
- [ ] 🧱 **Gate de latência intermitente no CI:** uma vez na `main` o p95 de entrega de status foi 29,7 s (limite 2 s) no job de carga; não reproduzi localmente (p95 < 1 s) e o mesmo código passou no PR. Se voltar, instrumentar a origem (outbox → fan-out → dispatcher) antes de relaxar o limite.
- [ ] 🔑 **Atualização do Evolution/Baileys:** sempre com número descartável (roteiro em `runbooks/PROVIDER-UPGRADE.md`); não há teste automatizado possível contra o WhatsApp real. O patch da busca de foto de perfil precisa ser revisto a cada versão.

Supply chain:
- [ ] 👤 O scan usa `ignore-unfixed: true` (um CRITICAL sem correção disponível não bloqueia): escolha de risco consciente, documentada no `RELEASE.md`.
- [ ] 🧱 Há achados `HIGH` na árvore de dependências do Evolution (axios, brace-expansion…) que este repositório não corrige sem um fork; o workflow só os lista.
- [ ] 🧱 O runner `ubuntu-latest` migra para Ubuntu 26 em 2026-10-19 (aviso do GitHub): rodar o CI e o release nele antes disso.

## 3. Operação em escala (G3)

- [ ] 🧱 Kubernetes: `PodDisruptionBudget`, `NetworkPolicy`, migração do banco como **Job** (hoje `AUTO_MIGRATE=true` nas réplicas; é seguro por lock consultivo, mas o Job separa responsabilidades no upgrade e no rollback), `updateStrategy: OnDelete` no StatefulSet do Evolution (troca de node sob comando, com `drain`; ver `runbooks/PROVIDER-UPGRADE.md`).
- [ ] 🧱 Dashboards e SLOs sobre as métricas que já existem (latência de entrega de eventos, `relayplane_unknown_*`, `relayplane_event_outbox_*`, `relayplane_event_projection_*`, `relayplane_webhook_backlog_overflow_total`).
- [ ] 👤 Backup e PITR do Postgres; capacidade real (carga em escala, não só o ensaio de 2000 mensagens).
- [ ] 🧱 Rotação da chave admin e log de auditoria; `govulncheck` no CI (R34).

## 4. Limpeza

- [ ] 👤 **GitGuardian:** marcar como falso positivo os incidentes abertos pelos PRs fechados #22 a #25 (linhas `POSTGRES_PASSWORD` do compose do sandbox; o compose agora não tem senha).
- [ ] 👤 Apagar branches remotas antigas, sem PR aberto: `feature/erasure-tombstone`, `feature/standalone-sandbox`, `feature/standalone-sandbox-v2`, `feature/sandbox-standalone`, `feature/sandbox-standalone-2`, `fix/scan-what-is-published`. Conferir `feature/agent-contract-quickwins-v2` (anterior a esta rodada).

## 5. Ideias futuras (fora do foco atual)

**Grupos.** Hoje existem `chat_id` só em grupo, `from` = telefone do autor (resolvido por `participantAlt`), `sender_lid`, `reply_to_provider_message_id` e a subscription com `exclude_groups` (**grupos são entregues por padrão**). O que poderia vir:

- Decisão: um filtro `chat_types: ["direct","group"]` (e `group_ids`) em vez de um booleano `include_groups`. Motivo: `sequence` é por assinatura e instância, e um grupo barulhento atrasa os chats diretos da mesma instância; duas assinaturas (uma de diretos, outra de grupos) têm filas independentes. E decidir se o padrão das existentes continua entregando grupos.
- Aditivo: `chat_type` sempre presente; `quoted_from_me` pelo próprio ledger (limite: não vê o que a pessoa mandou do celular); filtros `chat_types`/`group_ids`; envio para grupo (aceitar o JID em `to`, hoje exige 8 a 20 dígitos); marcar como lido em grupo (o Evolution pede o `participant`); simulador com grupo, citação e menção; registrar no contrato que mensagens de grupo carregam texto de **terceiros** (LGPD).
- Com número real: `mentioned`/`mentions` (`contextInfo.mentionedJid` contra a identidade da instância, número **e** LID); mensagens de sistema do grupo não virarem `message.received`; envio com menções.
- Mais adiante: metadados `group{subject,participants}` (desligável: o nome do grupo é dado pessoal), eventos `group.updated`/`group.removed`, limites de grupo em `GET /api/v1/limits`.

**Outras:**
- Consulta de mensagem por chave de idempotência (`GET /messages?idempotency_key=`), para quem precisar reconciliar sem reenviar.
- Entrega de eventos a consumidores internos novos (hoje o `event_outbox` é lido pelo fan-out e pelo projetor; um terceiro consumidor usaria o mesmo padrão: claim com lease e marco próprio).
