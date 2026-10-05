# Contrato mínimo (o que quem consome precisa saber)

Quem consome o RelayPlane (hoje, o `conversation_agent`) usa **cinco verbos, cinco eventos e três regras**. Todo o resto (Evolution, Baileys, outbox, lease, fencing, nodes) é interno e não faz parte do contrato.
Detalhes: [`openapi.yaml`](./openapi.yaml), [`EVENTS.md`](./EVENTS.md), [`events/events.schema.json`](./events/events.schema.json).

## 1. Cinco verbos

| Quero… | Chamada (SDK) | Resultado |
|---|---|---|
| ter um número | `instances.create(name)` → `instances.get_qrcode(id)` (ou `get_pairing_code`) | instância `CONNECTED` depois do pareamento |
| enviar | `messages.send_text(instance, to, text, reply_to=, idempotency_key=)` / `send_media(...)` (anexo antes: `media.upload`) | `message_id`; o resto chega por evento |
| baixar um anexo recebido | `media.download(media_id)` | bytes |
| mostrar que li / estou digitando | `messages.mark_read(...)`, `instances.send_presence(...)` | — |
| decidir uma mensagem incerta | `messages.list("UNKNOWN")` → `messages.resolve(id, sent=)` | a fila da instância volta a andar |

Sempre mande `idempotency_key` em envios: repetir a chamada (timeout, retry) devolve a mesma mensagem, não uma segunda.

## 2. Cinco eventos (webhook assinado, `subscriptions.create(url, ...)`)

| Evento | Quando | O que você faz |
|---|---|---|
| `message.received` | alguém escreveu (texto, mídia, resposta citada, grupo) | processa; responde |
| `message.outbound_status` | ciclo de vida de uma mensagem **sua**: `ACCEPTED`, `DELIVERED`, `READ`, `FAILED`, `UNKNOWN` | atualiza o estado; trata `UNKNOWN` (regra 3) |
| `message.deleted` | a pessoa apagou para todos | invalida decisões tomadas sobre aquela mensagem |
| `message.status` | recibo cru do provedor | normalmente ignora (use `outbound_status`) |
| `instance.status_changed` | a sessão mudou; `LOGGED_OUT` | `LOGGED_OUT` ⇒ chame `instances.get_qrcode` e peça novo QR ao titular |

Envelope fixo: `schema_version`, `event_id`, `sequence`, `event_type`, `tenant_id`, `instance_id`, `timestamp`, `payload`. O SDK valida e recusa versão que não conhece.

## 3. Três regras

1. **At-least-once: deduplique por `event_id`.** Responda 2xx só depois de guardar o evento; qualquer outra resposta (ou silêncio) provoca nova entrega, com o mesmo `event_id` e o mesmo `sequence`.
2. **Ordene por `sequence`, não por chegada.** É por (assinatura, instância), sem buracos. Um número que não chega é uma entrega que falta (pode estar no DLQ: `redeliver`). `SequenceTracker` do SDK faz isso. Rajadas de mensagens do mesmo segundo podem vir fora de ordem do WhatsApp: trate como bloco (debounce).
3. **`UNKNOWN` exige uma decisão.** Um envio ambíguo segura as mensagens seguintes **da mesma instância** (outras seguem). Verifique no aparelho e resolva como `sent` ou `not_sent`; na dúvida, pergunte a uma pessoa. Política de exemplo: `relayplane.unknown` ([runbook](./runbooks/UNKNOWN-MESSAGES.md)).

## 4. Erros e limites que o consumidor vê

* `429` por limite do tenant (respeite `Retry-After`); `413` para anexo acima do limite (`GET /limits`); `409/422` para estado ou argumento inválido (o SDK levanta exceções tipadas); `503` quando uma dependência está fora: tente de novo com a **mesma** `idempotency_key`.
* Entrega ao seu webhook: tentativas com backoff, depois DLQ; `subscriptions.pause/resume` para manutenção (nada se perde, a ordem se mantém). Seu endpoint precisa ser `https` público em produção.
* Apagar uma pessoa (LGPD): `contacts.erase(numero)`.

## 5. O que o consumidor **não** precisa saber

Evolution/Baileys e suas versões, nodes, fencing e epochs, outbox, leases, partições, retenção do barramento, migrações. Se algum desses vazar para o seu código, isso é um defeito do RelayPlane, não uso incorreto.

## 6. Garantias e limites **hoje** (honestos)

| Garantia | Estado |
|---|---|
| Envio: aceito de forma durável antes da resposta, uma vez por `idempotency_key`, ordem por instância | ✅ testado (contrato, sistema, caos) |
| Entrega do evento ao seu webhook: at-least-once, `sequence` sem buracos, retry/DLQ | ✅ testado |
| Mensagem **recebida** → seu webhook | ✅ **durável do aceite até a entrega ao tenant**: o broker é só transporte, as entregas são criadas a partir do banco (uma perda do Redis depois do aceite não perde nada para o tenant). Em detalhe, **durável a partir do aceite**: o 200 ao provedor só sai depois de a mensagem estar gravada no banco (chave de dedupe e evento na mesma transação); queda do processo ou do Redis depois disso só atrasa a entrega, nunca a perde. Depende do reconciler rodando (alerta `RelayPlaneEventOutboxStalled`). Testado contra Postgres/Redis/S3 reais |
| Apagamento por pessoa | ✅ apaga o que existe **e** marca o contato (para sempre, só um HMAC do número): evento, `message.deleted` ou anexo já em voo (Redis, outbox, fan-out, entrega já arrendada, download) é descartado, não recria o dado. **Um POST que já saiu para a rede antes do retorno não é recolhido.** Uma mensagem posterior ao pedido é entregue. Não alcança logs, backups, o estado do WhatsApp/Evolution nem a cópia que fica no Redis até ser consumida |
| Comportamento com WhatsApp real | ⚠️ validado com um número em 2026-10; mídia/citação/"digitando"/leitura ainda sem conferência no aparelho; sem teste automatizado possível ([atualização de versões](./runbooks/PROVIDER-UPGRADE.md)) |
| Pausa de assinatura | ✅ nada sai depois do `pause`, nem o que já estava arrendado; as entregas esperam e saem na ordem depois do `resume`. Uma pausa longa não guarda texto para sempre: o que continua pendente é apagado após `RETENTION_PENDING_DELIVERIES` (30 dias), deixando buracos em `sequence` |

Esta tabela é a fonte do que o consumidor pode assumir. Quando um ⚠️ fechar, ele vira ✅ aqui, no mesmo PR que o fecha.

## 7. Estabilidade do contrato

* Campo opcional novo: sem aviso, `schema_version` igual (o schema proíbe campos não declarados, então toda adição fica registrada).
* Remover, renomear ou mudar o significado: nova `schema_version`; o CI falha se código, schema e exemplos divergirem.
* Por ora há um só consumidor: o SDK e o contrato evoluem juntos no mesmo repositório, sem política de suporte a versões antigas.
