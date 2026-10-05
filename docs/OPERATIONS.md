# Runbook operacional

## Componentes e escala

| Componente | Réplicas | Observações |
|---|---|---|
| `gateway` | N (stateless) | API + webhooks atrás de um load balancer. `AUTO_MIGRATE=true` aplica migrations (lock consultivo no PostgreSQL). |
| `worker` | N | `docker compose up -d --scale worker=3`. Partições do broker são divididas por lease; adicionar/remover workers rebalanceia sozinho em segundos. |
| `reconciler` | 1..N | Réplicas são seguras (lease por instância + CAS no catálogo). 2 réplicas para HA. |
| `evolution-node-XX` | **exatamente 1 por identidade** | Singleton stateful. Escalar = **adicionar** `node-03`, `node-04`… (novo container, novo banco, nova entrada em `PROVIDER_NODES`), nunca replicar. |
| `postgres`, `redis`, object store | conforme provedor | PostgreSQL é a fonte de verdade; Redis guarda comandos/eventos em trânsito; o object store guarda mídia (qualquer backend S3). |

Portas: gateway `HTTP_PORT` (8080); worker/reconciler expõem `/metrics` e `/health/*` em `OPS_PORT` (9090).

## Eventos para o tenant (webhooks de saída)

O tenant se inscreve com `POST /api/v1/subscriptions {url, event_types?, instance_ids?}` e passa a receber `message.received` (com `reply_to_provider_message_id`,
e o `timestamp` do envelope é o carimbo do **provedor**), `message.outbound_status` (`ACCEPTED`, `DELIVERED`, `READ`, `FAILED`, `UNKNOWN` das mensagens que ele enviou,
com `accepted_at` e `sequence_no`), `message.status`, `message.deleted` e `instance.status_changed`. QR codes e violações de ownership nunca saem da plataforma.

**Caminho:** a mudança de status da mensagem grava o evento na tabela `event_outbox` **na mesma transação** → o reconciler publica no bus (a cada ~1 s) → o worker (`webhook-fanout`)
cria uma entrega por assinatura (`UNIQUE(subscription_id, event_id)`: reentrega do bus nunca duplica) → o dispatcher faz o POST assinado.

* **Assinatura:** `X-RelayPlane-Signature: v1=<hex>` = HMAC-SHA256(`<timestamp>.<body>`) com o segredo `whsec_…` (mostrado só na criação e na rotação; derivado de `SUBSCRIPTION_SECRET`/`WEBHOOK_SECRET`, não fica no banco).
  O consumidor deve rejeitar timestamps fora de ~5 min e **deduplicar por `X-RelayPlane-Event-Id`**. `POST …/rotate-secret` emite outro segredo; por 24 h as requisições levam as duas assinaturas.
* **Garantia:** pelo menos uma vez. Retry com backoff (5 s, 30 s, 2 min, 10 min, 30 min, 1 h, 2 h, 4 h, 8 h; ±20 %) e depois **DLQ** (`GET …/deliveries?status=DEAD`, `POST /deliveries/{id}/redeliver`).
  Circuit breaker por assinatura (5 falhas seguidas → pausa de 30 s…5 min, **sem gastar tentativas** dos itens na fila). Ordem: no máximo uma entrega em voo por (assinatura, instância), melhor esforço; um retry pode chegar depois de eventos mais novos, então o consumidor **reordena por `sequence`** (`1, 2, 3…` por assinatura e instância, sem buracos; uma reentrega mantém o número) e vê o que faltou. Contrato completo em [`EVENTS`](./EVENTS.md).
* **Segurança (SSRF):** em produção a URL deve ser `https` e resolver para endereço **público**; o IP é validado **no momento da conexão** (derrota DNS rebinding), redirects nunca são seguidos, não há proxy ambiente e a resposta é lida só até 64 KiB.
  Em desenvolvimento (`APP_ENV≠production`) `http://` e redes privadas são aceitos; force com `WEBHOOKS_ALLOW_PRIVATE_DESTINATIONS` / `WEBHOOKS_ALLOW_INSECURE`.
* **Idempotência de envio:** a janela em que a mesma `Idempotency-Key` devolve a mesma mensagem é `IDEMPOTENCY_TTL` (padrão **24 h**, mínimo 1 min). Um cliente que reenvia depois disso cria **outra** mensagem.
* **Criação idempotente:** `POST /subscriptions` aceita `Idempotency-Key`; repetir a chamada (um script de deploy que roda duas vezes) devolve a MESMA subscription
  (`200` + `Idempotent-Replayed: true`) **sem o segredo**; se ele foi perdido, use `rotate-secret`. A mesma chave com outro corpo é `422`.
* **Anexos recebidos:** a mensagem com mídia é gravada na fila `inbound_media` (no lugar de ir direto ao bus) e o `worker` a resolve: pede os bytes ao node
  (`/chat/getBase64FromMediaMessage`, que recebe a própria mensagem, então o node **não** precisa guardar mensagens), grava no object store, registra o `blob_metadata`
  (`READY`, retenção `INBOUND_MEDIA_TTL`, padrão 7 dias) e só então publica o `message.received` com `media`. Cada passo é idempotente e a linha é a fonte da verdade:
  um worker que cai deixa a lease expirar (3 min) e outro continua; com o bus fora do ar só a publicação se repete. Tentativas: nó indisponível até 6 (5 s, 15 s, 1 min, 5 min, 15 min),
  nó que responde mas não consegue baixar 3; depois o evento sai com `FAILED`. `INBOUND_MEDIA_MAX_BYTES` (padrão 25 MiB; negativo desliga) limita o que é baixado:
  o tamanho **anunciado** acima do limite é recusado sem baixar nada. A resposta da Evolution traz o arquivo em base64 dentro do JSON, então cada download ocupa na memória
  do worker cerca de 3x o tamanho do arquivo; dimensione `INBOUND_MEDIA_MAX_BYTES` com isso em mente (4 downloads em paralelo). Métricas: `relayplane_inbound_media_total{outcome}`,
  `relayplane_inbound_media_pending{stage}`. Download pelo tenant: `GET /api/v1/media/{id}/content`.
* **Chaves de API (R09):** um tenant tem **várias** chaves ao mesmo tempo (até 10 ativas), cada uma com nome, prefixo visível, `last_used_at` (gravado no máximo 1×/min) e,
  opcionalmente, validade. **Rotação sem parada:** `POST /api/v1/api-keys {name}` (o segredo `rpk_…` aparece uma única vez) → implante a nova → `DELETE /api/v1/api-keys/{id}` na antiga
  (as duas valem no intervalo; a revogada deixa de valer na hora). A **última chave utilizável não pode ser revogada** (409): um tenant não se tranca para fora por engano; o
  administrador sempre pode emitir outra (`POST /api/v1/tenants/{id}/api-keys`). Os hashes são o que fica no banco; as chaves que já existiam foram migradas como `initial`.
* **Limite de requisições por tenant (R10):** balde de fichas por tenant (`API_RATE_PER_SECOND`, padrão 50, e `API_RATE_BURST`, padrão 100; `0` desliga). Estourou: `429`
  com `Retry-After` e `RateLimit-Limit/Remaining`; os outros tenants não sentem. O balde mora no processo do gateway, então com N réplicas o limite efetivo é até N× (dimensione, ou
  ponha um limitador compartilhado na frente). Métrica `relayplane_api_rate_limited_total`.
* **Backpressure por subscription (R10):** `POST /subscriptions/{id}/pause` segura as entregas (os eventos continuam sendo enfileirados, **nada se perde**; o `GET` mostra
  `backlog.pending` e `oldest_pending_seconds`) e `…/resume` envia o acumulado em ordem de `sequence`. **Lease e recuperação:** o dispatcher arrenda lotes de 4× a concorrência (32) por 30 s (`Lease`); se o worker morre, as entregas dele (e, pela ordem por instância, as seguintes da mesma instância)
  esperam o lease acabar. A entrega leva `X-RelayPlane-Delivery-Claim` (1 = caminho saudável; maior = um worker morreu com ela) além de `X-RelayPlane-Delivery-Attempt`. Cada subscription pode ter no máximo
  `WEBHOOK_MAX_PENDING_PER_SUBSCRIPTION` (padrão 100 000; 0 = sem limite): um consumidor que fica fora do ar não enche o banco; ao atingir o limite as entregas **novas** dessa assinatura nascem na **DLQ** (`DEAD`, com o motivo, mantendo o `sequence`, então o buraco é visível, e reenviáveis por `redeliver` quando o consumidor voltar), com a métrica `relayplane_webhook_backlog_overflow_total` e o alerta `RelayPlaneWebhookBacklogOverflow`; e o limite de assinaturas por tenant (`WEBHOOKS_MAX_PER_TENANT`) agora é atômico sob requisições concorrentes. `WEBHOOK_MAX_IN_FLIGHT_PER_SUBSCRIPTION` (padrão 32, a capacidade de 4 workers; 8 limitava uma assinatura grande a 8 POSTs no total) POSTs em voo: um consumidor lento não ocupa o dispatcher inteiro.
* **Retenção e apagamento (R11, LGPD):** `RETENTION_MESSAGES` (padrão 90 dias; vale também para `UNKNOWN`, que perde destinatário e texto mas continua resolvível) e `RETENTION_PENDING_DELIVERIES` (padrão 30 dias: entregas ainda pendentes, tipicamente de uma assinatura deixada em pausa, são apagadas e a assinatura vê buracos em `sequence`): mensagens enviadas que já terminaram perdem **destinatário e texto** (a linha do ledger, com status e
  sequência, fica); `RETENTION_DEAD_DELIVERIES` (padrão 30 dias): a **DLQ**, que guarda o texto do usuário, é apagada; entregas já feitas seguem em `WEBHOOK_DELIVERED_RETENTION`
  (7 dias) e anexos recebidos em `INBOUND_MEDIA_TTL` (7 dias). `0` mantém para sempre. Métrica `relayplane_retention_applied_total{kind}`.
  **Apagamento por pessoa:** `DELETE /api/v1/contacts/{número}/data` (qualquer grafia: `+55 (62) 99999-9999`) apaga, **para o tenant que pediu**: destinatário e conteúdo das mensagens que ele
  enviou àquela pessoa (e cancela as que ainda não saíram), os eventos que a mencionam (entregues, pendentes e da DLQ) e os arquivos que ela enviou (objeto e metadados). É idempotente e
  devolve contagens, nunca o número. **O que estava em voo não volta:** o apagamento deixa uma **marca** (`contact_erasures`: só um **HMAC** do número com `ERASURE_KEY`, nunca o número, e **guardada para sempre**: uma linha pequena por contato, e nada limita a idade de um evento que chega a um consumidor, então qualquer prazo tornaria a garantia falsa depois dele): um evento aceito *antes* do pedido que ainda estava
  no Redis, no outbox, no fan-out ou **já arrendado por um dispatcher** (a entrega é descartada antes do POST), um `message.deleted` do contato, ou um anexo que estava sendo baixado, é descartado por quem o segura (e o arquivo já gravado é removido) em vez de recriar o dado depois do pedido. **Limite:** um POST que já saiu para a rede antes de o apagamento retornar não pode ser recolhido. **`ERASURE_KEY` precisa ficar estável** (vazia, deriva do `WEBHOOK_SECRET`): trocá-la faz as marcas antigas deixarem de casar, e um evento muito antigo que reaparecer não seria reconhecido. Uma mensagem que a
  pessoa manda **depois** do pedido é dado novo e é entregue normalmente. A marca vale pelo relógio dos serviços (aceite e pedido): mantenha-os sincronizados (NTP).
  **O que isso não alcança** (e deve constar na sua política): as entradas já gravadas nas streams transitórias do Redis (comandos e eventos em trânsito, aparadas por tamanho
  em `EVENT_BUS_RETENTION`; elas são descartadas ao serem consumidas, mas ficam no Redis até lá), a cópia do comando de uma mensagem que está nas mãos do provedor no instante do pedido (some quando ela resolve), o estado do próprio WhatsApp/Evolution,
  os logs (não carregam texto nem o número) e os backups do banco. Uma subscription que ainda não recebera eventos apagados verá **buracos em `sequence`**: depois de um apagamento
  isso é esperado.
* **Latência de entrega (R12):** `relayplane_event_delivery_lag_seconds{event_type,attempt}` é o tempo entre o RelayPlane **saber** do evento e o `2xx` do consumidor
  (mensagem recebida: da aceitação do webhook do provedor, ou do fim do download se tiver anexo; status de envio: da mudança de status). `attempt="first"` é o caminho saudável e
  tem **meta de p95 < 2 s**; `attempt="recovered"` é a entrega que ficou presa ao worker que morreu com ela (esperou o lease de 30 s); `attempt="retry"` inclui o backoff. Só `first` entra na meta.
  Medido pelos próprios dispatchers na carga multiprocesso **sem kill** (457 msg/s aceitas, 10 % de falha injetada no consumidor): **p95 ≤ 0,5 s**; o CI falha acima de 2 s
  (`LOAD_MAX_EVENT_LAG_P95=2s`, `tools/loadcheck/lag.py`). Com kill de worker a cada poucos segundos a cauda sobe até o lease (30 s): é recuperação de falha, limitada, e fica fora da meta. O intervalo de publicação do outbox de eventos passou de 1 s para
  `OUTBOX_INTERVAL` (padrão 250 ms), que é o maior componente da latência dos status. A origem dos status é o relógio do **banco**: um desvio entre banco e workers entra como erro de medida
  (lag negativo conta como zero), então mantenha NTP nos dois.
* **Presença e leitura (R07):** `POST /instances/{id}/presence {to, state: composing|recording|paused, duration_ms}` mostra "digitando…" / "gravando áudio…" (202 na hora; o node segura o estado por
  `duration_ms`, padrão 3 s, máx. 25 s, e **pausa sozinho**; no máximo 4 em curso por instância) e `POST /messages/read {instance_id, chat, provider_message_ids}` marca mensagens como lidas (síncrono, 1 a 50).
  As duas exigem a instância `CONNECTED` e vão direto ao provedor (não passam pela fila de envio: são efêmeras e não têm ordem em relação às mensagens). A leitura serve a conversas diretas: a Evolution
  descarta o `participant` de mensagens de grupo.
* **Resposta citada (R08):** `payload.reply_to` no `POST /messages/send` cita uma mensagem: por `provider_message_id` (a mensagem do usuário, com o `text` dela para a prévia) ou por `message_id` (uma mensagem
  nossa, já `ACCEPTED`; o RelayPlane preenche id do provedor, texto e `from_me`). **Atenção:** o node não guarda histórico; com só o id ele envia a resposta **sem a citação, em silêncio**, por isso a prévia
  viaja na requisição (cortada em 1024 caracteres; sem texto, a citação sai como caixa vazia). O texto da prévia é parte do payload e portanto some com a retenção e com o apagamento por pessoa.
* **Filtro de grupos:** `exclude_groups: true` descarta `message.received` de conversas em grupo para aquela subscription.
* **Trace:** o webhook leva o header `traceparent`. Os eventos de status (`message.outbound_status`) carregam o trace do `POST /messages/send` que criou a mensagem
  (o consumidor liga seu trace ao do envio); eventos que nascem no provedor (mensagem recebida) levam o trace da própria entrega.
* **`GET /api/v1/limits`:** janela de idempotência (`idempotency_retention_seconds`), limites de texto e mídia, ritmo de envio padrão e garantias de webhook
  (`retry_max_attempts`, `retry_horizon_seconds`). O cliente deve conferir na partida que o seu horizonte de retry fica abaixo da janela de idempotência
  (o SDK Python tem `Limits.assert_retry_horizon_within_idempotency`).
* **`GET /messages/{id}`** expõe `provider_message_id`, `accepted_at` e `error_message`: é o id ao qual o `reply_to_provider_message_id` de uma resposta se refere,
  e permite reconciliar por polling um evento que não chegou.
* **Operação:** métricas `relayplane_webhook_*` e `relayplane_event_outbox_published_total`; alertas `RelayPlaneWebhookDeadLetters`, `…Backlog`, `…CircuitOpen`, `RelayPlaneEventOutboxStalled`.
  Entregas concluídas são apagadas após `WEBHOOK_DELIVERED_RETENTION` (7 d); a DLQ nunca é apagada sozinha.

## Testes de caos

`make test-chaos` injeta falhas na infraestrutura real (docker compose pause/restart de Redis e PostgreSQL, workers cancelados em voo) enquanto
há tráfego, e depois confere: nada preso em `QUEUED`/`DISPATCHING`, nenhuma mensagem chega duas vezes ao provider, ordem por instância preservada,
e os únicos estados finais são `ACCEPTED` ou `UNKNOWN` (resolvível via `POST /messages/{id}/resolve`). Rode antes de atualizar Redis/PostgreSQL ou o adapter do broker.

### Jitter de rede e carga

* `TestJitter_SlowAndUnstableNetwork`: todo pacote para PostgreSQL e Redis recebe 5–45 ms de atraso (proxy TCP no teste). Converge, mas é **lento**
  (80 mensagens em ~65 s): cada mensagem faz vários round-trips ao banco, então latência de rede entre os workers e o PostgreSQL/Redis multiplica. Mantenha-os na mesma região/AZ.
* `TestJitter_ConnectionResetsUnderTraffic`: conexões resetadas 6 vezes durante o tráfego, sobre jitter. Sem perda nem duplicata; o cliente que repete um `Send`
  ambíguo com a mesma `Idempotency-Key` recebe a mesma mensagem.
* `make test-load` (`LOAD_MESSAGES`, `LOAD_INSTANCES`): linha de base numa máquina local de desenvolvimento (Windows, Docker, 20 instâncias, 2000 mensagens, 6 consumidores,
  provider falso sem latência): aceitação ~550 msg/s (p50 34 ms, p95 49 ms, p99 65 ms); entrega completa ~190 msg/s, com ordem por instância e sem duplicata.
  Use como referência de regressão, **não** como capacidade de produção: o provider real (Evolution/WhatsApp) e o hardware mudam o resultado.

### Carga multi-processo (`make test-load-stack`)

Sobe gateway, N workers e reconciler como **containers separados** (compose + `deploy/docker/compose.load.yml`) sobre PostgreSQL/Redis/object store reais; os nós do
provider viram `cmd/loadstub` (pareamento instantâneo, latência de envio configurável, contadores de entrega). O `cmd/loadgen` cria tenant e instâncias pela API
pública, envia as mensagens e confere do lado do provider, entre processos: **zero duplicatas, zero fora de ordem, nenhuma mensagem perdida**.
`WORKERS`, `INSTANCES`, `MESSAGES`, `STUB_SEND_LATENCY` ajustam o cenário; `CHAOS_KILL=1` dá SIGKILL num worker aleatório a cada 4 s (e o reinicia): envios em voo
viram `UNKNOWN` (nunca reenviados; o teste aceita `UNKNOWN` e confere que o provider recebeu entre `ACCEPTED` e `ACCEPTED+UNKNOWN`).

Medido (Docker local no Windows, 60 instâncias, 3000 mensagens, provider com 50 ms por envio):

| `COMMAND_PARTITIONS` | entrega ao provider |
|---|---|
| 8 | ~83 msg/s |
| 32 (padrão) | ~165–200 msg/s, **igual com 1, 3 ou 12 workers** |
| 128 | ~325 msg/s |

**O teto é o número de partições, não de workers**: cada partição processa uma mensagem por vez, então a vazão ≈ `min(partições, instâncias ativas) / tempo_por_mensagem`.
Adicionar workers só ajuda até haver uma partição por worker. Para mais vazão, aumente `COMMAND_PARTITIONS` (e tenha instâncias suficientes: duas mensagens da mesma
instância nunca são processadas em paralelo). A partição de uma instância é `fnv32a(instance_id) % N`: **mude `N` apenas com a fila vazia** (`relayplane_outbound_queue_depth` = 0);
reduzir `N` deixa streams antigos sem consumidor.
O `loadgen` também sobe um receptor de webhook próprio (`WEBHOOK_FAIL_RATE`, padrão 10 %, responde 500 a uma fração das requisições), assina a subscription do tenant e confere que **todo** status
de mensagem chegou como evento, com assinatura válida, apesar das falhas injetadas e dos kills de workers (1000 mensagens, 114 respostas 500 injetadas: 1000 eventos distintos recebidos, 0 assinaturas inválidas).
Com 6 kills em ~1 min (6000 mensagens): 5988 `ACCEPTED`, 12 `UNKNOWN`, 5999 envios no provider, 0 duplicatas, 0 fora de ordem.

## Object store (S3-compatível)

O core só conhece a porta `BlobStore`; o adapter `adapters/blob/s3` fala o protocolo S3 padrão, então o backend é uma decisão de implantação
(`BLOB_STORE_ENDPOINT`, `BLOB_STORE_ACCESS_KEY/SECRET_KEY`, `BLOB_STORE_BUCKET`; `COMPOSE_PROFILES` escolhe qual sobe no compose).
RustFS e SeaweedFS são exercitados pela mesma suíte (contrato do adapter + suíte de sistema inteira) com `make test-integration-s3`.

| Backend | Licença | Observações |
|---|---|---|
| **RustFS** `1.0.1` (padrão) | Apache-2.0 | Binário único, S3 completo para o que usamos (put/get/stat/list/delete, URL assinada, lifecycle). Estável (1.0) há pouco tempo: acompanhe os releases e rode `make test-integration-s3` ao atualizar. |
| **SeaweedFS** `4.47` | Apache-2.0 | Maduro (volume store + filer + gateway S3), escala bem; topologia mais pesada em produção (master, volume servers, filer, S3). **Cada bucket é uma *collection* que pré-aloca volumes**: use um bucket só (como o RelayPlane faz) e ajuste `-master.volumeSizeLimitMB`/`-volume.max` ao disco, senão aparece `We encountered an internal error` (sem volumes livres). |
| MinIO community (legado) | AGPL-3.0 | Imagens oficiais saíram do Docker Hub e o repositório no quay.io passou a exigir autenticação: **não dá mais para baixar a imagem** (só funciona onde ela já está em cache). Mantido como perfil opt-in; fora da matriz padrão e do CI. |

Trocar de backend em produção = copiar os objetos (`rclone sync`/`mc mirror`) e apontar o endpoint; os `object_key` e os metadados no PostgreSQL não mudam.
O `Put` do adapter é estrito quanto ao tamanho declarado em qualquer backend (o MinIO truncava corpo maior em silêncio).

## Configuração

Ver `.env.example`. Obrigatórias: `DATABASE_URL`, `BLOB_STORE_*`, `WEBHOOK_SECRET`, `ADMIN_API_KEY` (gateway),
`PROVIDER_NODES` (JSON: `id, provider, endpoint, api_key, capacity`). **Segredos não têm default** e as imagens são pinadas
por digest ([VERSIONS](VERSIONS.md)). `BLOB_STORE_PUBLIC_ENDPOINT` deve ser definido **só no gateway** (URLs assinadas para
clientes); workers usam o endpoint interno, que é o que os nodes Evolution alcançam para baixar mídia.

## Operações comuns

```bash
# estado dos nodes (admin)
curl -H "Authorization: Bearer $ADMIN" :8080/api/v1/nodes
# parar de alocar novas instâncias num node (sessões existentes ficam onde estão)
curl -X POST -H "Authorization: Bearer $ADMIN" :8080/api/v1/nodes/node-01/drain
curl -X POST -H "Authorization: Bearer $ADMIN" :8080/api/v1/nodes/node-01/resume
# rate policy (override por tenant/instância; campos 0 herdam do nível superior)
curl -X PUT -H "Authorization: Bearer $ADMIN" :8080/api/v1/tenants/<tenant_id>/rate-policy -d '{"min_interval_ms":1500,"max_per_minute":20}'
curl -X PUT -H "Authorization: Bearer $KEY"   :8080/api/v1/instances/<id>/rate-policy     -d '{"min_interval_ms":300}'
```

### Adicionar um node
1. Suba o container `evolution-node-03` (nova API key, novo banco `evolution_node_03`).
2. Acrescente-o a `PROVIDER_NODES` e reinicie gateway/reconciler/worker (o seed é idempotente; o node nasce `STARTING` e o
   Reconciler o promove a `READY` após a primeira sonda bem-sucedida).

### Reposição de um node que caiu
Veja [NODE-REPLACEMENT](./runbooks/NODE-REPLACEMENT.md): mesmo banco reconecta sem QR.

### Drenar/aposentar um node
`drain` ⇒ nenhuma instância nova. Para esvaziar: `POST /instances/{id}/migrate` para cada instância (veja abaixo).
**Atenção:** sessões Evolution são locais ao node; a migração faz *logout* no node antigo (fencing físico) e cria a sessão
no novo, exigindo **novo pareamento (QR)**. A operação fica em `VERIFY_CONNECTION` até a sessão conectar.

### Migração e `MIGRATION_BLOCKED`
`POST /api/v1/instances/{id}/migrate` (opcional `{"target_node_id":"node-02"}`) devolve `operation_id`. Acompanhe com
`GET /api/v1/operations/{id}` (`step`: `MIGRATION_REQUESTED → … → CONNECTED`).
`status=BLOCKED, error_code=FENCING_FAILED` significa que o owner antigo **não pôde ser provado fechado** (node fora do ar,
capability ausente). Nada foi trocado; a instância continua com o owner e o epoch antigos. Corrija o node e repita
`POST …/migrate` (retoma a mesma operação). Métrica/alerta: `relayplane_migration_blocked_total`.

### Mensagens `UNKNOWN` (barreira de ordem)
Runbook completo: [UNKNOWN-MESSAGES](./runbooks/UNKNOWN-MESSAGES.md). Listar: `GET /api/v1/messages?status=UNKNOWN`.
Um envio ambíguo (timeout/5xx após possível envio, worker morto no meio) vira `UNKNOWN` e **bloqueia as mensagens seguintes daquela
instância** (alerta: `relayplane_outbound_barrier_deferrals_total{reason="unknown"}` crescendo). Confirme no aparelho e resolva:
```bash
curl -X POST -H "Authorization: Bearer $KEY" :8080/api/v1/messages/<id>/resolve -d '{"outcome":"sent"}'      # ou "not_sent"
```
Por padrão (`UNKNOWN_BARRIER_TIMEOUT=0`) a barreira só cai com `resolve`: ordem estrita, ao custo de a instância ficar parada até alguém
decidir (monitore `relayplane_outbound_barrier_deferrals_total{reason="unknown"}`). Definir um timeout (ex.: `15m`) faz a barreira cair
sozinha; a mensagem segue `UNKNOWN` e a ordem relativa a ela deixa de ser garantida.

### Outbox de eventos (mensagens recebidas e status)
Todo evento público para o tenant (mensagem recebida, recibo, mudança de sessão, status de envio, mídia resolvida, reconciliação) **nasce no `event_outbox`**, na mesma transação do fato que o produz (a chave de dedupe, a mudança de estado, o fechamento do job de mídia), e só então o provedor recebe 200. Nenhuma regra de domínio publica direto no Redis.
Dois consumidores leem essa tabela, cada um com o seu marco:
* **o worker** cria as entregas do tenant a partir do **banco** (`fanout_at`; não passa pelo Redis). Por isso **o worker precisa estar rodando** para o tenant ser avisado, e uma queda ou perda do Redis não atrasa nem perde nada para ele. Métricas: `relayplane_event_outbox_pending` e `relayplane_event_outbox_oldest_seconds` (eventos aceitos sem entrega criada); alerta `RelayPlaneTenantFanOutStalled` (> 30 s);
* **o reconciler** publica no barramento a cada `OUTBOX_INTERVAL` (padrão 250 ms) para os consumidores **internos** (`published_at`); alerta `RelayPlaneEventOutboxStalled`.
Uma linha só é purgada (depois de 24 h) quando os dois marcos existem: a entrega ao tenant não depende de nenhuma cópia que viva só no Redis.

### Outbox
`outbox` guarda cada comando aceito até a publicação (gateway publica de imediato; o reconciler varre a cada 1 s e republica comandos
perdidos pelo broker). Entradas despachadas são purgadas após 24 h. Backlog crescente ⇒ broker indisponível: o accept continua funcionando.

### Retenção do EventBus (SLA)
O stream de eventos é cortado por tamanho (`EVENT_BUS_RETENTION`, padrão 100 000 eventos, corte aproximado). **Contrato:** um consumer group só
não perde eventos enquanto seu *lag* ficar abaixo da retenção. Dimensione assim: `retenção ≥ taxa_pico_de_eventos/s × pior_indisponibilidade_tolerada_s × 2`
(ex.: 50 ev/s e 30 min de indisponibilidade tolerada ⇒ 180 000). Métricas: `relayplane_eventbus_stream_length`, `…_retention_entries`,
`…_consumer_lag{group}`, `…_oldest_pending_seconds{group}`, `…_events_lost{group}` e `…_trim_risk` (pior lag ÷ retenção; ≥ 1 = perda).
Alertas (`deploy/prometheus/alerts.yml`): aviso em 50 %, página em 90 %, página imediata se `events_lost > 0`. Se um group perdeu eventos,
reconstrua o estado a partir do catálogo (o Reconciler já corrige `observed_state`; mensagens recebidas perdidas não são recuperáveis do
bus: tratar como incidente). Para um log de eventos durável de verdade, o caminho é trocar o adapter do EventBus (Kafka) sem mudar o core.

### DLQ de comandos
Mensagens que esgotaram os retries ficam `FAILED/RETRIES_EXHAUSTED` no catálogo e o comando em `relayplane:dlq`:
```bash
redis-cli XREVRANGE relayplane:dlq + - COUNT 20
```
Eventos que falham em todos os consumidores 10× vão para `relayplane:events:dead`. `UNKNOWN` (resultado ambíguo) **não** é
reenviado automaticamente: confirme no WhatsApp e, se necessário, reenvie com nova `Idempotency-Key`.

## Observabilidade

* `/metrics` (Prometheus). Regras de alerta de referência: [`deploy/prometheus/alerts.yml`](../deploy/prometheus/alerts.yml).
* Alertas prioritários: `relayplane_ownership_violation_total` (possível split-brain), `relayplane_assignment_epoch_mismatch_total`,
  `relayplane_migration_blocked_total`, `relayplane_reconciliation_drift_total` persistente,
  `relayplane_provider_node_health == 0`, `relayplane_outbound_queue_depth` crescente, `relayplane_outbound_dlq_total`.
* Logs JSON com `trace_id`, `tenant_id`, `instance_id`, `message_id`, `node_id`, `provider`, `assignment_epoch`, `operation_id`.
  Segredos e corpos de mensagem são redigidos. Defina `LOG_LEVEL=debug` somente temporariamente.
* Tracing: `OTEL_EXPORTER_OTLP_ENDPOINT=http://collector:4318`.

## Backup e recuperação

* PostgreSQL: backup regular (catálogo, assignments, operações, mensagens). É a única fonte de verdade do ownership.
* Redis: AOF habilitado no compose; perder o Redis perde comandos *em trânsito*, mas nunca mensagens aceitas: o outbox
  (PostgreSQL) republica, em ordem, os comandos despachados há mais de 2 min cujas mensagens seguem `QUEUED`.
* Volumes dos nodes Evolution (`evolution_node_XX`): contêm as credenciais das sessões; faça backup do banco do node.

## Restart/atualização

* Gateway/worker/reconciler: *rolling restart* seguro. Workers devolvem partições ao encerrar; comandos em andamento
  ficam pendentes e são reentregues (idempotentes).
* Evolution: atualize **um node por vez**, com versão pinada; use `drain` antes. Não existe upgrade global destrutivo:
  novas instâncias podem ir para `evolution-v3` quando o adapter existir, as atuais permanecem em v2.
* Migrations: aditivas e idempotentes (`schema_migrations`).
