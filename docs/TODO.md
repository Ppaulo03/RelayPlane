# TODO

O que **falta** no RelayPlane e em volta dele, em ordem aproximada de prioridade. O que já foi entregue está em [`AGENT-READINESS`](./AGENT-READINESS.md) (requisitos R01 a R26) e o estado das garantias em [`CONTRACT`](./CONTRACT.md) (seção 6). Atualize este arquivo no mesmo PR que fecha um item.

Legenda: 🔑 precisa de número de WhatsApp real · 👤 decisão ou ação sua (não do código) · 🧱 só código/CI.

## 1. Antes de colocar conversas reais de clientes

- [ ] 🔑 **Validação no aparelho** do que hoje só foi provado com simulador e formatos capturados: mídia nos dois sentidos, resposta citada, "digitando", confirmação de leitura. Repetir também a queda do node com o mesmo banco (reconecta sem QR) com o stack de hoje.
- [x] 🔑 ~~Confirmar a correção do atraso de 60 s~~ entre mensagens seguidas (busca de foto de perfil do Evolution, desligada na imagem): **confirmado** em 2026-10-05 com número real (antes: degraus de 60 s; depois: 1 a 2 s); ver `SPIKE-FINDINGS`. Falta só chegar a um release (o `0.3.0-rc2` ainda tem a imagem antiga).
- [ ] 🔑 **Casos do roteiro R22 ainda não vistos:** migração de instância com QR, reação, enquete, localização, queda de conexão **sem** logout, vídeo grande (100 MB) para medir memória do ingestor.
- [ ] 👤 **Primeiro release oficial:** escolher o número (`0.3.0`?), alinhar `docs/VERSIONS.md` (ainda cita 0.2.0), criar a tag `v<versão>` (gera os artefatos e o release no GitHub). Os candidatos `0.3.0-rc1` e `-rc2` já estão públicos no GHCR: não reutilize esses números.
- [ ] 👤 **Quem implanta** (decidido: não é do módulo): host, DNS, TLS, segredos, backup e restauração do Postgres e dos bancos dos nodes, e **bloquear `/metrics`** no ingress (está na porta pública do gateway).

## 2. Integração com o `conversation_agent` (repositório dele)

O adaptador dele foi escrito contra um contrato de gateway que **não é** o do RelayPlane. A tradução vive na borda do agente; o RelayPlane não muda.

- [ ] 🧱 Webhook: aceitar `X-RelayPlane-Signature: v1=<hex>` com o timestamp em header próprio (hoje espera `x-relay-signature: t=…,v1=…`); mesmo HMAC sobre `<ts>.<corpo>`.
- [ ] 🧱 Evento: traduzir o envelope (`event_id`, `instance_id`, `timestamp`, `sequence`, `payload.{from,chat_id,reply_to_provider_message_id,media{media_id}}`) para o `InboundEvent`; `event_id` é a chave de dedupe e `sequence` vira `source_sequence`.
- [ ] 🧱 Envio: `POST /api/v1/messages/send` com `{instance_id,to,text,reply_to}`; o estado `ACCEPTED`/`DELIVERED`/`FAILED`/`UNKNOWN` chega por `message.outbound_status`, não na resposta.
- [ ] 🧱 Reconciliar `UNKNOWN`: sem `GET ?idempotency_key=` no RelayPlane, o `lookup` vira **replay do mesmo POST** (mesma chave, mesmo corpo; vale dentro de 24 h) e a decisão usa a política de `relayplane.unknown` (padrão: perguntar a um humano).
- [ ] 🧱 Tratar eventos que hoje seriam ignorados: `instance.status_changed` (`LOGGED_OUT` pausa o canal e avisa) e `message.deleted` (invalida decisões sobre a mensagem).
- [ ] 🧱 Teste de ponta a ponta contra o sandbox publicado (`relayplane-sandbox-<v>.tar.gz`): mensagem entra, agente responde, status volta.
- [ ] 🧱 Substituir o `scripts/sandbox.py` dele pelo pacote do sandbox; remover o `docs/RELAYPLANE_CONTRACT.md` citado no código (não existe) em favor do `CONTRACT.md` daqui.

## 3. Grupos (pedido do agente: `conversation_agent/docs/RELAYPLANE_GROUPS_SPEC.md`)

Já existe: `chat_id` só em grupo, `from` = telefone do autor (resolvido por `participantAlt`), `sender_lid`, `reply_to_provider_message_id`, subscription com `exclude_groups`. **Hoje grupos são entregues por padrão**, ao contrário do que o doc supõe.

**Decisões pendentes (👤):**
- [ ] Usar um filtro `chat_types: ["direct","group"]` (e `group_ids`) no lugar do booleano `include_groups`. Motivo: `sequence` é por assinatura e instância, e um grupo barulhento atrasa os chats diretos da mesma instância; duas assinaturas (uma de diretos, outra de grupos) têm filas independentes.
- [ ] O padrão para assinaturas **existentes** continua entregando grupos?

**Fase 1: aditiva, quase toda provável no sandbox (🧱):**
- [ ] `chat_type` sempre presente (`@g.us` grupo; `@s.whatsapp.net`/`@lid` direto; `@broadcast`/`@newsletter`/desconhecido `other`).
- [ ] `quoted_from_me` pelo próprio ledger (a mensagem citada é uma que o RelayPlane enviou); sempre presente. Limite: não vê o que a pessoa mandou do celular, fora do RelayPlane.
- [ ] Filtro de assinatura `chat_types` e `group_ids`.
- [ ] Envio para grupo: aceitar o JID do grupo em `to` (hoje exige 8 a 20 dígitos), idempotência e `reply_to` em grupo.
- [ ] Marcar como lido em grupo: o Evolution pede o `participant` do autor; a API de leitura precisa dele.
- [ ] Simulador com grupo, citação e menção; critérios de aceite 1, 2, 4, 5, 6, 8 e 9 do doc do agente.
- [ ] Contrato: mensagens de grupo carregam texto de **terceiros**; registrar isso (LGPD) em `CONTRACT`/`OPERATIONS`.

**Fase 1b: depende de número real (🔑):**
- [ ] `mentioned` e `mentions`: `contextInfo.mentionedJid` comparado com a identidade da própria instância (número **e** LID, aprendidos no evento de conexão e guardados; compara contra qualquer um). Testar com número e com LID.
- [ ] Mensagens de sistema do grupo (entrada, saída, assunto) **não** virarem `message.received`: confirmar com captura real e fixture.
- [ ] Envio com `mentions` (Evolution aceita lista de mencionados; não validado).

**Fase 2 (🔑):**
- [ ] Metadados `group{subject,participants}` (desligável: o nome do grupo é dado pessoal; consulta ao Evolution e cache).
- [ ] Evento `group.updated`/`group.removed` (instância removida do grupo), habilitando os eventos de grupo no node.
- [ ] Limites específicos de grupo em `GET /api/v1/limits`.

## 4. Operação em escala (G3)

- [ ] 🧱 Kubernetes: `PodDisruptionBudget`, `NetworkPolicy`, migração do banco como **Job** (hoje `AUTO_MIGRATE=true` nas réplicas; é seguro por lock consultivo, mas o Job separa responsabilidades no upgrade e no rollback), `updateStrategy: OnDelete` no StatefulSet do Evolution (troca de node sob comando, com `drain`; ver `runbooks/PROVIDER-UPGRADE.md`).
- [ ] 🧱 Dashboards e SLOs sobre as métricas que já existem (latência de entrega de eventos, `relayplane_unknown_*`, `relayplane_event_outbox_*`, retenção do barramento).
- [ ] 👤 Backup e PITR do Postgres; capacidade real (carga em escala, não só o ensaio de 2000 mensagens).
- [ ] 🧱 Rotação da chave admin e log de auditoria; `govulncheck` no CI (R34).
- [ ] 🧱 O barramento é Redis Stream com retenção por tamanho (não é log durável). Se for preciso replay histórico grande, trocar o adaptador (Kafka) sem mudar o core.

## 5. Qualidade e riscos conhecidos

- [ ] 🧱 **Teste instável sob `-race`:** `TestEventDeliveryLag_RetriesAreSeparatedFromTheHealthyPath` falha ~2 em 120 execuções na `main` (a amostra de latência do retry cai no balde de 25 ms, e o teste espera pelo menos 250 ms de backoff). Na falha, os dois pedidos do consumidor aparecem com `At` fora de ordem (o de tentativa 2 *antes* do de tentativa 1); ainda não achei a causa (suspeita: carimbo `At` do `Receiver` de teste, ou relógio). Reproduzir com `go test -race -count=250 -run <teste> ./internal/systemtest` num container Go. Não é do bloco 3 (reproduz sem ele).
- [ ] 🧱 **Gate de latência intermitente no CI:** uma vez na `main` o p95 de entrega de status foi 29,7 s (limite 2 s) no job de carga; não reproduzi localmente (p95 < 1 s do lado do dispatcher) e o mesmo código passou no PR. Se voltar, instrumentar a origem (outbox → barramento → fan-out → dispatcher) antes de relaxar o limite.
- [ ] 🧱 **`UNKNOWN` resolvido pelo próprio RelayPlane:** consultar o node (o Evolution expõe consulta de mensagem?) para saber se a mensagem existe e resolver sozinho, em vez de parar a instância até alguém decidir. Só vale se a consulta for confiável; validar com número real.
- [ ] 👤 **Reenvio muito antigo depois do TTL do dedupe:** um webhook antigo reapresentado ganha `AcceptedAt = agora` e a marca o trata como dado novo. Usar também o carimbo do provedor fecha o caso, mas descartaria uma mensagem que a pessoa mandou *antes* do pedido e que chegou depois (celular offline). Escolha de política de privacidade.
- [ ] 🧱 Apagamento de contato depende de **relógios sincronizados** entre os serviços (a marca compara a hora do aceite com a do pedido) e **não alcança** o que já está gravado no Redis até ser consumido, logs, backups nem o estado do WhatsApp/Evolution. Revisitar se a política de privacidade exigir mais.
- [ ] 🔑 **Atualização do Evolution/Baileys:** sempre com número descartável (roteiro em `runbooks/PROVIDER-UPGRADE.md`); não há teste automatizado possível contra o WhatsApp real.
- [ ] 🧱 O runner `ubuntu-latest` migra para Ubuntu 26 em 2026-10-19 (aviso do GitHub): rodar o CI e o release nele antes disso.

## 6. Limpeza

- [ ] 👤 **GitGuardian:** marcar como falso positivo os incidentes abertos pelos PRs fechados #22 a #25 (linhas `POSTGRES_PASSWORD` do compose do sandbox; o compose agora não tem senha).
- [ ] 👤 Apagar branches remotas antigas, sem PR aberto: `feature/erasure-tombstone`, `feature/standalone-sandbox`, `feature/standalone-sandbox-v2`, `feature/sandbox-standalone`, `feature/sandbox-standalone-2`, `fix/scan-what-is-published`. Conferir `feature/agent-contract-quickwins-v2` (anterior a esta rodada).
- [ ] 👤 `archive.zip` aparecia na raiz dos dois repositórios (e sumiu do RelayPlane durante o trabalho): confirmar quem o gera e se deve entrar no `.gitignore`.
