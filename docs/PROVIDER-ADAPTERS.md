# Provider adapters

## Contrato

`ports.MessagingProvider` (`internal/ports/provider.go`):

```go
CreateInstance(ctx, CreateInstanceRequest{Assignment, TenantID, Name}) (*ProviderInstance, error)
DeleteInstance(ctx, Assignment) error
GetInstanceState(ctx, Assignment) (*InstanceState, error)
GetPairingCode(ctx, Assignment) (*PairingCode, error)
SendMessage(ctx, Assignment, OutboundMessage) (*SendResult, error)
Capabilities(ctx) ProviderCapabilities
// extensões necessárias para reconciliação e fencing físico:
ConnectInstance(ctx, Assignment) error
Disconnect(ctx, Assignment) error      // NÃO retorna nil até o socket estar confirmado fechado
ProbeNode(ctx, nodeID) (*NodeProbe, error)
```

`ports.WebhookAdapter`: `Provider()`, `Authenticate(req) → NodeClaim`, `Normalize(req) → []events.Inbound`.

Regras para qualquer adapter:

1. Nenhum tipo do provider sai do pacote; tudo vira tipo/erro canônico.
2. Erros canônicos (`core/errs`): `ProviderUnavailable` (provadamente não agiu ⇒ retry), `AmbiguousDispatch` (pode ter agido),
   `InstanceNotFound`, `InstanceAlreadyExists` (permite *adotar* após crash), `AuthenticationFailed`, `PairingUnavailable`,
   `StaleAssignment` (se o provider detecta), `InvalidRecipient`, `ProviderRejected`, `CapabilityMissing`.
   Falha de *dial* ⇒ `Unavailable`; falha depois de escrever o request em um **send** ⇒ `Ambiguous`.
3. `CreateInstance` é determinístico no identificador (aqui, `instanceName = instance_id`) para que "já existe" signifique adoção.
4. `Capabilities` é honesto; endpoints devolvem `501 capability_not_supported` quando falta.
5. A autenticação do webhook não confia na query: o token é por node e o chamador valida a claim contra o catálogo.

## `ProviderContractSuite`

`internal/contracttest/provider.go`. Todo provider precisa passar: criar/estado/duplicado, inexistente, pairing, envio de texto e
mídia, socket morto ⇒ retryable mas node saudável, tradução de erros (unavailable/auth/not-found/ambiguous), `Disconnect`
com confirmação, delete, `ProbeNode`, stale assignment (se suportado). Uso:

```go
contracttest.ProviderContractSuite(t, func(t *testing.T) contracttest.ProviderHarness {
    // backend fake do provider + funções Inject/Pair/Drop
})
```

Já executam a suíte: `memory.FakeProvider` e o adapter Evolution v2 (contra um fake HTTP fiel às rotas da v2.3.7).

## Evolution API v2 (`adapters/providers/evolution/v2`)

| Operação | Rota Evolution |
|---|---|
| criar | `POST /instance/create` (`instanceName`, `integration=WHATSAPP-BAILEYS`, `webhook{url, headers, events}`) |
| estado | `GET /instance/connectionState/{name}` (+ `GET /instance/fetchInstances?instanceName=` para distinguir *nunca pareado* de *desconectado* pelo `ownerJid`) |
| QR / pairing / reconectar | `GET /instance/connect/{name}` |
| fencing / logout | `DELETE /instance/logout/{name}` + reconfirmação de estado |
| delete | `DELETE /instance/delete/{name}` |
| enviar | `POST /message/sendText/{name}`, `sendMedia`, `sendWhatsAppAudio` (mídia por **URL assinada**, ou base64 *dentro da chamada ao node*, nunca pelo broker) |
| probe | `GET /` (versão fora da allow-list `v2.TestedVersions` = `2.3.7` ⇒ node não-READY) |

Webhook (`webhook.go`): `messages.upsert → message.received` (ignora `fromMe`), `messages.update → message.status`
(`SERVER_ACK→sent`, `DELIVERY_ACK→delivered`, `READ/PLAYED→read`), `connection.update → instance.status_changed`
(`close`+`401` ⇒ `LOGGED_OUT`), `qrcode.updated → instance.qrcode_updated` (**sem** material de QR). Eventos não modelados são descartados.

Verificado contra a imagem real `v2.3.7` **com Baileys 7.0.0-rc13** (`deploy/docker/evolution`, docker compose): create, QR, estado, delete, probe/versão e
entrega de webhooks `qrcode.updated`/`connection.update` autenticados. **Não verificado com conta WhatsApp real** (sem aparelho):
envio efetivo de texto/mídia e payloads de `messages.upsert/update` seguem a documentação e testes com fakes — risco registrado no ROADMAP.

Limitações assumidas (documentadas, não escondidas):
* A Evolution não oferece "fechar socket sem perder credenciais": o fencing usa `logout`, que invalida a sessão ⇒ migração entre nodes
  exige novo QR (a menos que os nodes compartilhem armazenamento de sessão — fora de escopo).
* A imagem oficial da Evolution `v2.3.7` embute Baileys vulnerável (CVE-2026-48063): use sempre a imagem derivada (ver VERSIONS).
* A Evolution não conhece `epoch`; `RejectsStale=false`. O fencing lógico é feito pelo worker antes da chamada.

## Versionamento (`evolution/v2` e `evolution/v3`)

Cada versão é um adapter registrado sob a sua chave (`evolution-v2`, futuro `evolution-v3`) em `bootstrap`; instâncias gravam
`provider`, nodes gravam `provider`/`provider_version`. O placement filtra por provider: novas instâncias ⇒ `DEFAULT_PROVIDER`
(`evolution-v3` quando existir), existentes continuam em v2. Não há upgrade global destrutivo. Para adicionar v3: novo pacote
`…/evolution/v3`, mesmo `ProviderContractSuite`, `Register("evolution-v3", …)`, nodes v3 no `PROVIDER_NODES`.
