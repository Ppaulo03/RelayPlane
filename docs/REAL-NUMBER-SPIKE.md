# Spike com número real de WhatsApp

Objetivo: **trocar suposições por fatos**. Tudo que o RelayPlane assume sobre o que a Evolution real envia foi testado só contra um servidor
falso escrito por nós. Este roteiro liga um número descartável a um node real da Evolution e grava o que ela realmente faz.

> **Use um número descartável, não o principal.** A Evolution/Baileys é uma API não oficial: o WhatsApp pode bloquear o número.
> O ambiente usa um ritmo de envio conservador (1 mensagem a cada 3 s, no máximo 10 por minuto).

## O que queremos descobrir

| # | Pergunta | Por que importa |
|---|---|---|
| **A1** | Onde vem o id da mensagem **citada** em uma resposta? (`contextInfo.stanzaId` em `extendedTextMessage`? em outro lugar?) O RelayPlane o extrai certo? | é a evidência de que um "sim" responde ao prompt de confirmação do agente (`INV-022`) |
| **A2** | O que é o `messageTimestamp` (segundos? milissegundos?) e quanto ele difere do horário de chegada? | comparação de tempos na confirmação e ordenação de rajadas |
| **A3** | Como é um áudio, imagem, documento, vídeo recebido? Tem URL, `directPath` + `mediaKey`, `base64`? | define o desenho da mídia inbound (R06) |
| **A4** | Que recibos (`messages.update`) chegam para as mensagens que enviamos, e com quais valores? | status `DELIVERED`/`READ` |
| **A5** | O que a Evolution envia quando o aparelho desconecta ou faz logout? | reconciliação e migração |
| **A6** | **Um node novo, com o mesmo banco, reconecta sem QR?** | o runbook de reposição de node |

## Passo a passo

Pré-requisitos: Docker, Python 3.10+ com `pip install -e sdk/python`, e o número descartável com o WhatsApp instalado.
Os comandos usam `sh`; no Windows rode no Git Bash. Se o seu Python não se chama `python`, exporte `PYTHON=...`.

### 1. Subir
```bash
sh deploy/docker/spike.sh up          # constrói e sobe (gateway, worker, reconciler, Postgres, Redis, Evolution node-01, tap)
```
Os segredos são gerados em `.env.spike` e as capturas ficam em `captures/` (ambos fora do git). Espere o `stack ready`.

### 2. Parear
```bash
sh deploy/docker/spike.sh pair
```
Salva e abre `captures/qr.png`. No celular: **WhatsApp > Aparelhos conectados > Conectar um aparelho** e leia o QR (ele se renova sozinho enquanto não for lido).
Termina com `CONNECTED`.

### 3. Mandar as mensagens (do celular **para o número pareado**, a partir de OUTRO número/aparelho)
Use um segundo WhatsApp (o seu) para conversar com o número descartável. Em ordem:

1. Um **texto** qualquer ("oi").
2. Do RelayPlane para o seu celular:
   ```bash
   sh deploy/docker/spike.sh send 55SEUNUMERO "Confirma amanhã às 15h? Responda CITANDO esta mensagem."
   ```
   (imprime o `provider message id`). No seu celular, **responda citando essa mensagem** (segure a mensagem > **Responder**) com "sim" (A1).
3. Responda **citando** uma mensagem **antiga** (não a última) e uma mensagem **sua** (cita a si mesmo).
4. Mande uma **nota de voz**, uma **imagem** (com legenda), um **documento** (PDF) e um **vídeo** curto (A3).
5. Mande uma mensagem **num grupo** onde o número descartável esteja (opcional, A5 de grupos).
6. **Leia** as mensagens que o RelayPlane enviou e deixe outra **entregue e não lida** (A4).
7. Envie do RelayPlane uma imagem para o seu celular: `sh deploy/docker/spike.sh send 55SEUNUMERO --file ./foto.jpg --caption "teste"`.

### 4. Ver o resultado
```bash
sh deploy/docker/spike.sh summary
```
Imprime, para cada pergunta A1–A5, o que a Evolution realmente fez (caminhos do JSON, unidade e atraso do timestamp, chaves de mídia, valores de recibo) e se o RelayPlane
extraiu o `reply_to` corretamente. **O relatório não contém texto de mensagem**: pode ser colado numa conversa.

### 5. Teste de reposição do node (A6)
```bash
sh deploy/docker/spike.sh restart-node       # recria o container do node MANTENDO o banco dele
sh deploy/docker/spike.sh logs evolution-node-01
```
Depois de um minuto, envie uma mensagem (`send`). Se chegar sem novo QR, a hipótese está confirmada. Se pedir QR de novo, anote a mensagem do log.

### 6. Compartilhar sem expor dados
Os arquivos em `captures/` têm números, nomes e textos reais. Para compartilhar ou commitar fixtures:
```bash
sh deploy/docker/spike.sh sanitize           # gera captures/sanitized/webhooks.jsonl
```
A estrutura é preservada (é o que serve para testes); números, ids, nomes, textos, URLs e chaves de mídia são trocados por valores estáveis (uma resposta citada continua
apontando para a mensagem que cita). **Revise o arquivo antes de enviar**: a limpeza é heurística.

### 7. Encerrar
```bash
sh deploy/docker/spike.sh down -v            # apaga os volumes (inclui a sessão do WhatsApp) — o aparelho ficará "conectado" até você removê-lo no celular
```
Remova o aparelho em **Aparelhos conectados** no celular.

## Resultado
O que foi observado, os defeitos encontrados e os limites que o consumidor precisa tratar estão em [`SPIKE-FINDINGS`](./SPIKE-FINDINGS.md).

## O que vem depois
Os payloads reais viram fixtures de ouro (`internal/adapters/providers/evolution/v2/testdata/`), os testes do adapter passam a usá-los, o simulador passa a gerar o formato
observado e as questões A1–A6 ficam respondidas no [`AGENT-READINESS`](./AGENT-READINESS.md) (R20).

## Como funciona
```
celular ⇄ Evolution (node real) ──POST /webhooks──► webhooktap ──encaminha──► gateway
                                                       │ grava o JSON bruto em captures/webhooks.jsonl
gateway/worker ──POST /sink (eventos normalizados)────►│ (o que um consumidor como o agente receberia)
```
O `webhooktap` não altera nada: encaminha a requisição original ao gateway e só registra. O header de token do node nunca é gravado.
O QR code aparece nas capturas até ser lido: trate `captures/` como sensível.
