# RelayPlane

Control Plane distribuído para **sessões stateful de mensageria** (WhatsApp e afins).
Distribui instâncias entre *Provider Nodes*, garante **ownership exclusivo** com `assignment_epoch`,
envia mensagens de forma **assíncrona e ordenada por instância**, normaliza eventos inbound e
reconcilia continuamente *estado desejado* × *estado observado*.

O primeiro provider é a **Evolution API v2**, o broker é **Redis Streams**, a persistência é **PostgreSQL**
e a mídia usa qualquer **object store compatível com S3** (Claim-Check; testado com RustFS e SeaweedFS; MinIO legado, opt-in). Nenhum deles é conhecido pelo domínio: são *adapters*
(Ports & Adapters / Hexagonal). O documento de design original está em
[`docs/design/original-design.md`](docs/design/original-design.md).

```
Adapters ──▶ Ports ◀── Core        (nunca Core ─▶ Evolution/Redis/PostgreSQL)
```

## Quick start

Pré-requisitos: Docker, Go 1.26+ (para desenvolver), Python 3.10+ (SDK).

```bash
cp .env.example .env            # troque todos os CHANGE_ME (openssl rand -hex 24)
docker compose up -d --build    # gateway, worker, reconciler, postgres, redis, rustfs (padrão), evolution-node-01/02
curl localhost:8080/health/ready
```

Fluxo completo (admin cria tenant → tenant cria instância → QR → envia mensagem):

```bash
ADMIN=<ADMIN_API_KEY do .env>
KEY=$(curl -s -X POST localhost:8080/api/v1/tenants -H "Authorization: Bearer $ADMIN" \
        -d '{"name":"Acme"}' | jq -r .api_key)                       # exibida uma única vez

curl -s -X POST localhost:8080/api/v1/instances -H "Authorization: Bearer $KEY" \
     -H 'Idempotency-Key: acme-main' -d '{"name":"Comercial"}'        # placement automático + criação no node
curl -s localhost:8080/api/v1/instances/<id>/qrcode -H "Authorization: Bearer $KEY"   # escaneie no WhatsApp
# o Reconciler/webhooks levam observed_state a CONNECTED
curl -s -X POST localhost:8080/api/v1/messages/send -H "Authorization: Bearer $KEY" \
     -H 'Idempotency-Key: order-1' \
     -d '{"instance_id":"<id>","to":"5562999999999","type":"text","payload":{"text":"Olá!"}}'
```

SDK Python ([`sdk/python`](sdk/python)):

```python
async with RelayPlaneClient("http://localhost:8080", api_key) as rp:
    inst = await rp.instances.get_or_create("Comercial")
    qr = await rp.instances.get_qrcode(inst.id)
    sent = await rp.messages.send_text(inst.id, "5562999999999", "Olá!", idempotency_key="order-1")
    media = await rp.media.upload("contrato.pdf")                       # Claim-Check: só a referência trafega
    await rp.messages.send_media(inst.id, "5562999999999", media.id, caption="Contrato")
```

## Desenvolvimento

```bash
make infra-up        # postgres/redis + RustFS/SeaweedFS de teste (portas 55440/56390/59011/59012; MinIO opt-in na 59010)
make test            # unit + contract + sistema (em memória): rápido, sem infraestrutura
make test-integration# adapters reais + suíte de sistema contra PostgreSQL+Redis+RustFS
make test-integration-s3 # o mesmo contra TODOS os object stores suportados (RustFS e SeaweedFS)
make test-race       # race detector (em container Docker golang: o Windows local não tem gcc)
make sdk-test        # SDK Python
```

| Camada | Pacote |
|---|---|
| Domínio puro (sem I/O, só stdlib) | `internal/core/{instance,ownership,routing,messaging,media,events,reconciliation,errs,ids}` |
| Contratos | `internal/ports` |
| Casos de uso | `internal/app`, `internal/worker`, `internal/reconciler`, `internal/ratelimit`, `internal/idempotency` |
| Transporte | `internal/api/http` |
| Adapters | `internal/adapters/{providers/evolution/v2, messaging/redisstreams, persistence/postgres, blob/s3, lock/redislock, memory}` |
| Composition root | `internal/bootstrap`, `cmd/{gateway,worker,reconciler}` |
| Suítes de contrato reutilizáveis | `internal/contracttest` (`ProviderContractSuite`, `RepositoryContract`, `CommandQueueContract`, …) |

## Documentação

| Documento | Conteúdo |
|---|---|
| [ARCHITECTURE](docs/ARCHITECTURE.md) | camadas, state machines, ownership/fencing, ordering, idempotência, claim-check, mapa invariante→teste |
| [OPERATIONS](docs/OPERATIONS.md) | runbook: deploy, escalar workers/nodes, drain, migração, DLQ, métricas e alertas |
| [FAILURE-MODES](docs/FAILURE-MODES.md) | o que acontece (e o que *não* acontece) em cada falha |
| [PROVIDER-ADAPTERS](docs/PROVIDER-ADAPTERS.md) | como escrever/versionar um provider; contrato e limitações da Evolution |
| [VERSIONS](docs/VERSIONS.md) | versões e digests pinados (Gateway, Evolution, Baileys, imagens) |
| [ROADMAP](docs/ROADMAP.md) | o que está feito, adiado e por quê |
| [openapi.yaml](docs/openapi.yaml) | especificação da API pública |

## Garantias (todas com teste automatizado)

`INV-01…INV-12` — ver mapa em [ARCHITECTURE §10](docs/ARCHITECTURE.md#10-invariantes-e-onde-são-testadas).
Resumo: um owner por instância; comando com epoch antigo nunca chega ao provider; operações idempotentes;
nodes `DRAINING` não recebem instâncias; eventos duplicados não geram efeitos duplicados; core não importa
adapters; ordem por `instance_id`; novo owner só após *physical fencing*; saúde do node ≠ saúde da instância;
payload binário nunca passa pelo broker; o Reconciler converge.

## Segurança

Tenant derivado da API key (nunca do payload); webhooks autenticados por token HMAC **por node** e validados
contra o assignment corrente (`OWNERSHIP_VIOLATION`); nenhum segredo em logs (`observability.Redact`); QR/pairing
nunca em eventos; chaves da API Evolution fora do catálogo; nenhuma imagem `:latest`.
