# Spike com número real: o que a Evolution realmente faz

Resultado do [roteiro](./REAL-NUMBER-SPIKE.md) executado em 2026-10-03 contra **Evolution 2.3.7 (Baileys 7.0.0-rc13)** com um número real.
Os payloads gravados, depois de sanitizados, estão em
[`internal/adapters/providers/evolution/v2/testdata/real`](../internal/adapters/providers/evolution/v2/testdata/real) e são verificados por
`golden_test.go`: é a fonte de verdade do formato, no lugar do servidor falso.

## Respostas às perguntas

| # | Pergunta | Resposta |
|---|---|---|
| A1 | Onde vem o id da mensagem citada? | `contextInfo.stanzaId` (no topo de `data` e dentro do corpo tipado). O `reply_to_provider_message_id` saiu correto em 100% dos casos (conversa direta e grupo). |
| A2 | Unidade e atraso do `messageTimestamp`? | Inteiro em **segundos**, resolução de 1 s. Atraso real de chegada ≈ 2,5 s. Mensagens do mesmo segundo têm timestamp igual. |
| A3 | Formato de mídia recebida? | Áudio, imagem, documento e vídeo trazem `url` + `directPath` + `mediaKey` (+ hashes); **nunca** base64. A `url` é do CDN do WhatsApp e o conteúdo é cifrado: baixar exige decifrar com `mediaKey` ou pedir à Evolution (R06). |
| A4 | Recibos? | `messages.update` com `keyId` + `status` ∈ `SERVER_ACK`, `DELIVERY_ACK`, `READ`, `PLAYED` (nota de voz tocada). |
| A5 | Desconexão/logout? | Logout pelo celular: `connection.update` com `state: close`, `statusReason: 401` → `LOGGED_OUT`. Durante o pareamento aparece `state: refused`, `statusReason: 428` (não modelado). |
| A6 | Node novo com o mesmo banco reconecta sem QR? | **Sim.** Recriar o container com o mesmo banco: `connecting` → `open`, nenhum `qrcode.updated`, envio seguinte aceito. |

## Defeitos que o spike expôs (todos corrigidos)

1. **Mídia enviada a partir do blob store falhava** (`400 Owned media must be a url or base64`): a Evolution valida `media` com `isURL`, que exige host
   com ponto, e a URL assinada tinha host interno (`rustfs:9000`). O adapter manda base64 quando o host não é aceito.
2. **Remetente em grupo vinha como LID** (`…@lid`, dígitos que não são telefone). O telefone está em `participantAlt` (e `remoteJidAlt` em conversa
   direta). `from` agora é o telefone; `chat_id` (JID do grupo) e `sender_lid` são campos novos.
3. **Exclusões de mensagem não eram assinadas.** `messages.delete` (status `DELETED`) agora vira o evento `message.deleted` com o id da mensagem apagada.
4. **`date_time` do envelope não é confiável**: a Evolution o formata em hora local do container com sufixo `Z` (3 h atrás com `America/Sao_Paulo`).
   O projetor descartou um logout real como "mais antigo que o catálogo". Os nodes passam a rodar com `TZ=UTC` e eventos sem timestamp próprio
   usam o horário de chegada.

## Limites que o consumidor precisa tratar

- **Edição é ilegível.** Uma mensagem editada chega como `messageType: secretEncryptedMessage` sem o texto novo (`type: secretEncrypted` no
  RelayPlane). Um "sim" editado depois de enviado não pode valer como confirmação: peça de novo.
- **Apagar é visível** (`message.deleted`): invalide qualquer confirmação baseada na mensagem apagada.
- **A ordem não é garantida com falhas.** Com o gateway fora do ar, a Evolution reenvia os webhooks ("Tentativa n/10", ≈ 9 s de intervalo) em paralelo:
  três mensagens do mesmo segundo chegaram como a, c, b e não há dado que reconstrua a ordem. Com o gateway saudável a rajada manteve a ordem.
  O envelope não tem número de sequência. O agente deve agregar rajadas (debounce) e não depender da ordem dentro do mesmo segundo.
- **Node parado não perde mensagens**: o que foi enviado durante a queda chega depois, em ordem, com o `messageTimestamp` original.

## Ainda não testado

Mensagem de enlace/enquete/reação/contato/localização, grupo com menção, mídia recebida de tamanho grande (o vídeo de teste tinha 100 MB),
perda de conexão sem logout, e o logout com o código corrigido (a causa foi provada pelos timestamps capturados e há teste de regressão).

## Mensagens seguidas aparecendo com 60 s de atraso (observado depois do spike)

**Sintoma:** mensagens enviadas uma após a outra chegavam com 60 s de intervalo.
**Causa (lida no código da imagem, 2.3.7):** o handler `messages.upsert` do Evolution faz, para cada mensagem, `await this.profilePicture(remoteJid)`, **sem nenhuma condição de configuração**, também para as mensagens enviadas (`fromMe`), dentro de um laço sequencial; o mesmo vale para `contacts.update`. `profilePicture()` chama o Baileys sem timeout e engole o erro, então, quando o WhatsApp não responde, espera o `defaultQueryTimeoutMs` do Baileys (**60 000 ms**). O próprio Baileys documenta um caso em que o servidor nunca responde a essa consulta (o `tctoken` do próprio JID).
**Correção:** a imagem desliga a busca (RelayPlane não usa foto de perfil) por um patch estrito; veja `runbooks/PROVIDER-UPGRADE.md`.

**Confirmado com número real (2026-10-05, mesma stack e mesma sessão, só a imagem do node trocada, banco mantido, reconectou sem QR):**

| | Mensagens que chegam juntas (recebimento) | Rajada de 6 envios |
|---|---|---|
| **Antes** (sem o patch) | a 1ª chega em ~1 s e **cada uma das seguintes chega 59,9 a 60,0 s depois da anterior**, com atraso acumulado de 23 s até 163 s em relação ao carimbo do provedor (11 mensagens diretas reais capturadas pela tap; as isoladas chegaram em ~1 s) | sem salto de 60 s: ~3 s entre entregas (o ritmo do próprio gateway) |
| **Depois** (patch) | as 4 mensagens mandadas em seguida chegaram em 1 a 2 s, no máximo 1,1 s entre elas | (não repetida: não havia o que corrigir) |

Ou seja, o atraso aparece no **recebimento** de mensagens próximas (a fila do handler espera a consulta de foto de cada uma, o webhook da mensagem sai antes da espera, por isso é a *seguinte* que atrasa); o envio em rajada não era afetado. Para medir de novo: `python tools/spike/latency.py` (envio) e a tap da stack (recebimento; atraso = hora de chegada do webhook menos `messageTimestamp`).

