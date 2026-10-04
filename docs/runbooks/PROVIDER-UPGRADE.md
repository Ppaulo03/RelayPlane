# Runbook: atualizar o Evolution e o Baileys

O Evolution é uma API **não oficial** sobre o Baileys, que por sua vez imita o cliente do WhatsApp. Uma versão nova pode mudar o formato dos webhooks, os campos aceitos pela API, o endereçamento (LID) ou o comportamento da sessão, sem aviso. Por isso a atualização é uma **mudança de contrato**, não um bump de dependência.

## O que está fixado e onde

| O quê | Onde |
|---|---|
| Imagem base do Evolution (tag + digest) | `deploy/docker/evolution/Dockerfile` |
| Baileys (versão exata, verificada no fim do build) | `ARG BAILEYS_VERSION` no mesmo Dockerfile |
| Versão que o adapter aceita (outra ⇒ node não-`READY`) | `v2.TestedVersions` em `internal/adapters/providers/evolution/v2/provider.go`; ampliável em runtime com `EVOLUTION_ALLOWED_VERSIONS` |
| Tag da imagem | `docker-compose.yml`, `deploy/staging/staging.sh`, `.github/workflows/evolution-image.yml`, `deploy/kubernetes/relayplane.yaml` |
| Tabela e matriz de compatibilidade | [`VERSIONS.md`](../VERSIONS.md) |

`internal/archtest/supply_chain_test.go` impede imagem oficial sem o ajuste, Baileys abaixo da correção do CVE-2026-48063 e tags flutuantes.

## O que costuma mudar (o que já nos mordeu)

Cada item abaixo foi um defeito real encontrado com o número de teste, e tem fixture ou teste: são os primeiros lugares onde uma versão nova quebra.

* **Formato dos eventos:** `date_time` (hoje hora local rotulada como `Z`; por isso usamos a hora de chegada), `participant`/`participantAlt`/`remoteJidAlt` (endereçamento LID), edição como `secretEncryptedMessage`, exclusão só com `MESSAGES_DELETE`.
* **Campos exigidos pela API:** URL de mídia precisa de host com ponto (senão base64), resposta citada precisa do conteúdo (senão é descartada em silêncio), `sendPresence` exige `number`, `presence` e `delay`, `markMessageAsRead` exige `{id, fromMe, remoteJid}`.
* **Sessão:** reconexão sem QR com o mesmo banco; logout derrubando a sessão.
* **Banco do Evolution (Prisma):** as migrações dele são só para frente e já falharam numa versão candidata (`2.4.0` / `homolog`: Prisma 7 sem `prisma.config` na imagem).

## Como detectamos uma quebra (do mais barato ao mais caro)

1. **Allow-list de versão:** uma versão não testada deixa o node fora de `READY`. Nada passa a rodar por acidente.
2. **Fixtures reais** (`internal/adapters/providers/evolution/v2/testdata/real/`, sanitizadas): o parser é testado contra o que o Evolution realmente enviou. Elas descrevem a versão **atual**: uma versão nova só é comparada com elas quando você captura o formato novo (passo 3); os testes não detectam sozinhos uma mudança.
3. **Simulador** (`internal/simulator`) e a suíte de contrato do provider: garantem que o *nosso* lado continua coerente, mas **não** provam nada sobre a versão nova do Evolution. Só provam que o adapter não regrediu.
4. **Smoke de staging** (CI, nó Evolution real sem número): sobe, fica `READY`, devolve QR e as guardas de produção funcionam.
5. **Número real** (único que vê o comportamento do WhatsApp): o roteiro abaixo.

Só os itens 2 e 5 olham o Evolution novo de verdade. Sem eles, passar no CI **não** significa que a versão serve.

## Procedimento

### 0. Decidir se vale atualizar
Atualize quando houver: CVE no Evolution/Baileys ou nas dependências (o scan do release barra `CRITICAL` com correção), correção de algo que nos afeta, ou o WhatsApp mudando o protocolo e a versão atual parando de conectar. Não atualize por rotina: cada atualização custa um ciclo de validação com número real.

### 1. Branch e imagem candidata
```bash
git checkout -b feature/evolution-<versão>
```
Altere a base (tag **e** digest) e/ou `BAILEYS_VERSION` no Dockerfile. Construa: `make evolution-image`. O build confere o Baileys e as cópias do `fast-xml-parser` na árvore final; o scan precisa passar.

### 2. Compatibilidade do banco (antes de tocar num node real)
O Evolution migra o próprio banco ao subir, só para frente. **Faça backup do banco de cada node** (`pg_dump` ou snapshot): o rollback de imagem *não* desfaz a migração. Teste a subida da imagem candidata primeiro num banco **vazio** e depois numa **cópia** do banco de um node.

### 3. Amostrar o formato novo e comparar
Com a imagem candidata e um número descartável, rode o spike (`tools/spike`) e capture os eventos; a sanitização (`sanitize.py`) gera fixtures sem dados pessoais. Compare com `testdata/real/`:
* diferenças de formato ⇒ ajuste o adapter e **acrescente** a fixture nova (não substitua a antiga se a versão atual ainda é suportada);
* nenhuma diferença ⇒ ótimo, mas continue com o roteiro: o formato igual não prova o comportamento igual.

### 4. Roteiro com número real (descartável)
Marque cada item; o resultado vai para um relatório versionado em `docs/` (como o [`SPIKE-FINDINGS`](../SPIKE-FINDINGS.md)).
- [ ] parear por QR; envio de texto e recebimento; receipts (enviado/entregue/lido)
- [ ] mídia nos dois sentidos (imagem, áudio/voz, vídeo, documento), grupo e conversa direta (LID)
- [ ] resposta citada nos dois sentidos; "digitando"; marcar como lido
- [ ] edição e exclusão chegam como eventos
- [ ] parar e subir o node **com o mesmo banco**: reconecta sem QR e o envio seguinte é aceito
- [ ] gateway parado e religado: nenhuma mensagem perdida ou duplicada
- [ ] logout é percebido (`LOGGED_OUT`) e não é confundido com evento antigo

### 5. Ampliar a allow-list e documentar
`TestedVersions` e [`VERSIONS.md`](../VERSIONS.md) (tabela, matriz, digest) só passam a aceitar a versão nova **depois** do passo 4. Ajuste o comentário do CVE se o motivo da atualização for esse.

### 6. Implantar um node de cada vez
1. `drain` do node (`POST /api/v1/nodes/{id}/drain`): nenhuma instância nova entra; as existentes continuam.
2. Troque a imagem **dele** (o banco fica). A sessão reconecta sem QR ([NODE-REPLACEMENT](./NODE-REPLACEMENT.md), caso 1).
3. Verifique `READY`, instâncias `CONNECTED`, um envio e um recebimento de teste, e `GET /api/v1/messages?status=UNKNOWN` (um envio cortado pela troca vira `UNKNOWN`: [UNKNOWN-MESSAGES](./UNKNOWN-MESSAGES.md)).
4. `resume` e, com o primeiro estável por tempo suficiente para você confiar (pelo menos algumas horas de tráfego real), repita no próximo.

Durante a janela com versões mistas, ambas precisam estar na allow-list.

### 7. Rollback
* **Imagem antiga com o banco antigo:** reaplique a imagem anterior; funciona se a versão nova **não** chegou a migrar o banco.
* **Banco já migrado:** restaure o backup do passo 2 **antes** de subir a imagem antiga; sem isso o Evolution antigo pode recusar o schema novo. Mensagens recebidas entre o backup e a restauração dependem do WhatsApp reenviar.
* Se a sessão cair de vez: QR de novo (caso 2 do runbook de reposição).

## Sinais de que a versão nova está errada (monitore nas primeiras horas)
`relayplane_provider_node_health`, queda de eventos recebidos, `message.status` que não avança para entregue/lido, aumento de `UNKNOWN`, instâncias em `DISCONNECTED`/`LOGGED_OUT` sem causa, falha de download de mídia, eventos sem `chat_id`/remetente.

## O que continua sem cobertura
* Não existe teste automatizado contra o WhatsApp real: a validação final é manual e com número descartável.
* Uma versão do WhatsApp pode quebrar a *versão atual* do Baileys sem que nada mude do nosso lado; nesse caso a atualização é forçada e o roteiro acima é o caminho mais rápido.
