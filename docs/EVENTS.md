# Contrato dos eventos para o tenant

O que o RelayPlane envia por webhook a cada assinatura. O contrato escrito é o JSON Schema
[`docs/events/events.schema.json`](./events/events.schema.json) (versão **1**); os exemplos em [`docs/events/examples`](./events/examples)
são validados contra ele no CI, junto com o que o código realmente emite e com o que a Evolution real produziu
(`testdata/real`). Entrega, assinatura, retry e DLQ estão em [`OPERATIONS`](./OPERATIONS.md#eventos-para-o-tenant-webhooks-de-saída).

## Envelope

| Campo | Descrição |
|---|---|
| `schema_version` | versão do contrato (hoje `1`). Muda **só** em mudança incompatível; um campo opcional novo não muda a versão |
| `event_id` | id estável do evento: o mesmo em toda reentrega. **Deduplique por ele** |
| `sequence` | `1, 2, 3…` por (assinatura, instância), **sem buracos**; uma reentrega mantém o número |
| `event_type` | `message.received`, `message.status`, `message.outbound_status`, `message.deleted`, `instance.status_changed` |
| `provider`, `tenant_id`, `instance_id` | origem |
| `timestamp` | `message.received`: carimbo do **provedor** (resolução de 1 s; empates são normais). Demais: quando o RelayPlane observou o fato |
| `source_assignment` | `{node_id, epoch}` do dono que produziu o evento (opcional) |
| `traceparent` | contexto de trace W3C (opcional; também vai no header) |
| `payload` | específico do tipo (abaixo) |

## Como usar `sequence`

* **Reordenar.** Um retry pode chegar depois de eventos mais novos da mesma instância. Ordene por `sequence`, não por chegada nem por `timestamp`.
* **Detectar o que faltou.** Um número que nunca chega é uma entrega que você não recebeu: pode estar no DLQ
  (`GET /subscriptions/{id}/deliveries?status=DEAD`, `POST /deliveries/{id}/redeliver`; a reentrega volta com o **mesmo** `sequence`).
* **A numeração é por assinatura.** Com `event_types` ou `exclude_groups` você simplesmente não recebe alguns eventos e a sua sequência continua sem buracos.
* O SDK Python traz `Event` e `SequenceTracker` (reordena, descarta reentregas, informa `missing()`, permite `skip_gap()` depois do seu próprio
  tempo limite e guarda o estado para sobreviver a reinício).

O que `sequence` **não** faz: não reconstrói a ordem em que as pessoas escreveram. Se três mensagens do mesmo segundo chegam ao RelayPlane
fora de ordem (o provedor reenvia webhooks em paralelo quando o gateway está indisponível, [verificado](./SPIKE-FINDINGS.md)), a ordem de chegada
é a que fica registrada. Trate rajadas como um bloco (debounce).

## Eventos

### `message.received`
Mensagem de uma pessoa. `from` é o **telefone** (o provedor às vezes endereça por LID; `sender_lid` guarda o LID e, em grupos, `chat_id` o JID do grupo).
`reply_to_provider_message_id` só existe em respostas citadas: é o `provider_message_id` de uma mensagem anterior (a sua, em `message.outbound_status`).
`type`: `text`, `image`, `audio`, `video`, `document`, `sticker` ou `secretEncrypted`.

> **`secretEncrypted` é uma edição ilegível.** Com a versão atual do provedor, quando a pessoa edita uma mensagem o novo texto não chega. Não trate
> um "sim" editado depois de enviado como confirmação: pergunte de novo.

### `message.deleted`
A pessoa apagou uma mensagem para todos. `provider_message_id` é o da mensagem apagada, como foi entregue em `message.received`:
invalide qualquer decisão tomada com base nela.

### `message.outbound_status`
Ciclo de vida de uma mensagem que **você** enviou (`ACCEPTED`, `DELIVERED`, `READ`, `FAILED`, `UNKNOWN`), por `message_id`. `provider_message_id` é o id que uma
resposta citada vai carregar. `sequence_no` é a ordem de **envio** da mensagem na instância (não confundir com `sequence`, da entrega).

### `message.status`
Recibo cru do provedor (`sent`, `delivered`, `read`, `failed`) por `provider_message_id`.

### `instance.status_changed`
Mudança de estado da sessão (`CONNECTED`, `LOGGED_OUT`, …). `LOGGED_OUT` pede novo pareamento.

## Evolução do contrato

* Campo **opcional** novo: entra no schema e nos exemplos, `schema_version` continua igual. O schema proíbe campos não declarados justamente para que
  toda adição seja registrada.
* Remover/renomear campo, tornar obrigatório um opcional ou mudar o significado: **nova versão**. O SDK recusa (`ValueError`) uma `schema_version` que não entende.
* O CI falha se o código emitir algo que o schema não descreve, se um tipo de evento ficar sem exemplo, ou se um exemplo deixar de ser válido.

## Exemplo

```json
{
  "schema_version": 1,
  "event_id": "evt_01m4234qz0k2d1y9s3xv7t8w6n",
  "sequence": 12,
  "event_type": "message.received",
  "provider": "evolution-v2",
  "tenant_id": "tenant_…",
  "instance_id": "inst_…",
  "timestamp": "2026-10-03T23:11:34Z",
  "payload": {
    "provider_message_id": "3EB076A9503CA689E453E4",
    "reply_to_provider_message_id": "3EB0316FBDC6EC84F13164",
    "from": "5511999990000",
    "type": "text",
    "text": "sim"
  }
}
```
