# Sandbox: o RelayPlane inteiro sem número de WhatsApp

Para desenvolver e testar um consumidor (como o `conversation_agent`) contra os binários **reais** do RelayPlane (gateway, workers, reconciler, PostgreSQL, Redis,
object store) sem WhatsApp: o nó do provedor é trocado por um **simulador** que se comporta como a Evolution API para o RelayPlane e, do outro lado, deixa você
fazer o papel do usuário.

```bash
make sandbox-up        # sobe a stack (segredos gerados em .env.sandbox, ignorado pelo git)
make sandbox-example   # roda examples/sandbox/quickstart.py (precisa do SDK: pip install -e sdk/python)
make sandbox-down
make test-sandbox      # up + exemplo + down (é o que o CI roda)
```

## Sem clonar o repositório

Cada release publica `relayplane-sandbox-<versão>.tar.gz` (nos artefatos do run e, em release por tag, no release do GitHub). Ele traz o script, o compose e o `images.env` das imagens **daquele release**
(gateway, worker, reconciler e o simulador, todos por digest; os pacotes do GHCR são públicos), então só precisa de Docker e curl:

```bash
tar xzf relayplane-sandbox-0.3.0.tar.gz && cd sandbox-0.3.0
sh sandbox.sh up      # gera .env.sandbox; GATEWAY_PORT, SIM_NODE_01_PORT e SIM_NODE_02_PORT mudam as portas se as padrão (18080-18082) estiverem ocupadas
sh sandbox.sh info    # URLs e chaves
sh sandbox.sh down    # remove tudo, inclusive os dados
```

Sem o script: `docker compose --env-file .env.sandbox --env-file images.env -f compose.yml up -d --wait` (o `.env.sandbox` precisa de `ADMIN_API_KEY`, `WEBHOOK_SECRET`,
`EVOLUTION_NODE_01_API_KEY`, `EVOLUTION_NODE_02_API_KEY` e `BLOB_SECRET_KEY`). Não depende de nada do repositório (não constrói, não monta arquivos, não usa overlay); um teste de arquitetura garante isso.
Um consumidor rodando na sua máquina recebe os webhooks em `http://host.docker.internal:<porta>/...`. O que muda em relação ao `make sandbox-up`: usa imagens publicadas em vez de construir do código.
O `examples/sandbox/quickstart.py` roda igual contra ele (precisa do SDK Python).

Gateway `http://127.0.0.1:18080` · simulador do node-01 `http://127.0.0.1:18081` · node-02 `18082` (header `apikey` = `EVOLUTION_NODE_0X_API_KEY` de `.env.sandbox`).

## O que o simulador faz

* **Para o RelayPlane** ele é um node: cria instâncias, devolve QR, aceita envios (devolve um id de provedor), responde `connectionState`, `logout`, `delete`.
  O adapter real da Evolution passa **a mesma suíte de contrato** de provedores contra ele (`TestSimulatorPassesProviderContractSuite`).
* **Para você** ele expõe `/_sim/...`, e tudo que você aciona chega ao RelayPlane como um **webhook no formato real da Evolution**, passando pelo normalizador de produção:

| Chamada | Efeito |
|---|---|
| `POST /_sim/instances/{id}/scan` | o usuário lê o QR: a instância fica `CONNECTED` |
| `POST /_sim/instances/{id}/inbound` `{from, text, type, reply_to, group, push_name, timestamp}` | o usuário escreve; `reply_to` é o id de provedor da mensagem citada, ou `"last_sent"` para citar a última mensagem que o RelayPlane enviou. `type`: `text`, `image`, `audio`, `video`, `document`. Para anexos: `content` (base64, o que o download devolve), `mimetype`, `filename`, `seconds`, `declared_size` (o tamanho que a mensagem anuncia, para simular um arquivo enorme sem guardá-lo) e `fail_downloads` (os N primeiros downloads falham como um anexo que o WhatsApp já descartou) |
| `POST /_sim/instances/{id}/receipt` `{message_id, status}` | `delivered`, `read` ou `failed` (`message_id` aceita `"last_sent"`); com `SIM_AUTO_RECEIPTS=true` (padrão do sandbox) toda mensagem aceita vira `delivered` sozinha |
| `POST /_sim/instances/{id}/disconnect` `{logged_out}` | o socket cai (ou o usuário desloga o aparelho) |
| `GET /_sim/instances/{id}/sent` | tudo que o RelayPlane enviou ao node, inclusive a citação (`quoted_id`, `quoted_text`, `quoted_from_me`; `quote_dropped` quando o node teria enviado a resposta SEM citar, como o real faz quando só recebe o id) |
| `GET /_sim/instances/{id}/presences`, `GET /_sim/instances/{id}/reads` | os "digitando…" e as leituras que o RelayPlane pediu ao node |
| `POST /_sim/faults` `{next: [...]}` | falhas nas próximas chamadas à API do node: `unavailable`, `auth`, `not_found`, `server_error`, `ambiguous` |
| `GET /_sim/instances`, `POST /_sim/reset` | inspeção e limpeza |

Cada chamada de controle só responde depois que o gateway respondeu ao webhook (`gateway_status` na resposta): é determinístico, sem `sleep`.

## Exemplo mínimo (o que `quickstart.py` faz)

1. cria um tenant e uma subscription apontando para um receptor local (verifica a assinatura com `relayplane.verify_request`);
2. cria uma instância e "lê o QR";
3. envia uma mensagem e espera o webhook `message.outbound_status` (`ACCEPTED`);
4. responde **citando** essa mensagem e confere que o webhook `message.received` traz `reply_to_provider_message_id` igual ao id da mensagem enviada.

## Limites (o que o simulador NÃO prova)

* Ele reproduz o **formato de payload que o adapter espera**. Se a Evolution real emitir algo diferente (por exemplo, onde fica o `contextInfo.stanzaId` de uma resposta),
  o simulador não percebe: isso só se valida com um número real (**R20** do [`AGENT-READINESS`](./AGENT-READINESS.md)). Quando houver payloads reais, eles viram fixtures
  e o simulador passa a gerá-los.
* Não há latência, limites de taxa nem bloqueios do WhatsApp. Nunca use o simulador fora de desenvolvimento e CI.

`cmd/loadstub` (testes de carga) é o mesmo simulador com pareamento instantâneo e latência de envio configurável.
