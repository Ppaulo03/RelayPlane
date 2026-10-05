# Runbook: mensagens `UNKNOWN`

Para operadores e para o agente que usa o RelayPlane.

## O que é

Um envio **ambíguo** não permite saber se o destinatário recebeu: timeout ou 5xx depois de o provider poder ter enviado
(`error_code=AMBIGUOUS_DISPATCH`), ou worker morto no meio do envio (`WORKER_CRASH`). Reenviar pode **duplicar** a mensagem; não reenviar pode **perdê-la**. Por isso o RelayPlane não decide:
a mensagem fica `UNKNOWN` e as mensagens seguintes **da mesma instância** ficam retidas (ordem estrita) até alguém resolver. Outras instâncias não são afetadas.

## Como perceber

* Alerta `RelayPlaneUnknownMessageWaiting`: a `UNKNOWN` mais antiga espera há mais de 5 min (`relayplane_unknown_oldest_seconds`).
* `relayplane_unknown_messages`: quantas esperam; `relayplane_outbound_barrier_deferrals_total{reason="unknown"}` crescendo: mensagens retidas.
* Evento `message.status` com `UNKNOWN` (webhook do tenant) e a mensagem em `GET /api/v1/messages/{id}`.

## Encontrar

```bash
curl -H "Authorization: Bearer $KEY" "$GW/api/v1/messages?status=UNKNOWN"                       # as suas, da mais antiga à mais nova
curl -H "Authorization: Bearer $KEY" "$GW/api/v1/messages?status=UNKNOWN&instance_id=$INSTANCE"
```
```python
for m in await rp.messages.list("UNKNOWN", instance_id=instance.id):
    ...
```

## Decidir: `sent` ou `not_sent`

Verifique **no aparelho** (ou no histórico do WhatsApp da conta) se a mensagem está lá, com o texto e a hora esperados.

| O que você vê | Resolva como | Efeito |
|---|---|---|
| a mensagem aparece na conversa | `sent` | vira `ACCEPTED`; as seguintes seguem |
| a mensagem **não** aparece e a conexão estava ok | `not_sent` | vira `FAILED`; as seguintes seguem; reenviar é decisão sua (nova mensagem, nova chave de idempotência) |
| não dá para verificar (aparelho inacessível, sessão caiu) | **não resolva ainda** | resolver errado duplica ou perde |

Regras para não errar:
* **Na dúvida, prefira `not_sent` só para conteúdo idempotente ou de baixo risco** (lembrete repetível); para conteúdo que não pode duplicar (cobrança, confirmação) espere a verificação.
* `sent` com a mensagem de fato não enviada **perde** a mensagem em silêncio; `not_sent` com a mensagem enviada e um reenvio posterior **duplica**. Nenhum dos dois é detectável depois pelo RelayPlane.
* Decisão registrada: guarde quem resolveu e por quê (log de auditoria do seu lado).

```bash
curl -X POST -H "Authorization: Bearer $KEY" "$GW/api/v1/messages/$ID/resolve" -d '{"outcome":"sent"}'   # ou "not_sent"
```
```python
await rp.messages.resolve(message_id, sent=True)
```

## Política para o agente

O agente não deve resolver às cegas. Um exemplo de política (e seu teste) está em [`sdk/python/relayplane/unknown.py`](../../sdk/python/relayplane/unknown.py):

* `UnknownPolicy` decide por mensagem: `ASK_HUMAN` (padrão: notifica o responsável com o texto, o destinatário e a hora), `RESOLVE_NOT_SENT` (só para mensagens que o chamador marcou como seguras para reenviar) ou `RESOLVE_SENT`.
* O padrão é perguntar: o custo de uma pergunta é menor que o de uma mensagem duplicada ou perdida para um cliente.
* `resolve_unknown(client, policy)` varre `list("UNKNOWN")` e aplica a política; rode-o periodicamente e ao receber `message.status=UNKNOWN`.

## Timeout da barreira

`UNKNOWN_BARRIER_TIMEOUT` (padrão `0`) define se a barreira cai sozinha:
* `0`: ordem estrita; a instância espera por uma decisão (use com o alerta acima).
* ex. `15m`: a barreira cai depois do prazo; a mensagem **continua** `UNKNOWN` e a ordem relativa a ela deixa de ser garantida. Escolha isso quando disponibilidade importa mais que ordem.

## O que o runbook não cobre

Descobrir *por que* houve ambiguidade: veja o nó (`relayplane_provider_node_health`), os logs do worker (`message_id`, `node_id`) e [FAILURE-MODES](../FAILURE-MODES.md).
