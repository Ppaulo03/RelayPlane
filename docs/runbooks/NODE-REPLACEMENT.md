# Runbook: reposição de um node Evolution

Um node é um **singleton com estado**: o banco dele guarda as sessões do WhatsApp (credenciais). Trocar o processo mantendo o banco **reconecta sem QR**; perder o banco (ou migrar a instância para outro node) **exige novo QR**.

## Garantia de fencing: um processo por sessão

Duas instâncias do Evolution com a mesma sessão derrubam uma à outra (o WhatsApp encerra a conexão antiga). O desenho evita isso:

* **Kubernetes:** `StatefulSet` com identidade estável (`evolution-v2-0`, `-1`…). Cada pod usa **o seu próprio banco**, derivado do nome do pod (`$(EVOLUTION_DATABASE_URI_BASE)/$(POD_NAME)`); `internal/archtest` falha se alguém voltar a usar um URI único compartilhado.
  O controlador nunca roda dois pods com o mesmo nome ao mesmo tempo.
* **Cuidado:** `kubectl delete pod --force --grace-period=0` e a perda de um *worker node* do Kubernetes podem deixar o processo antigo vivo enquanto o novo sobe. Não force a exclusão de um pod de node de WhatsApp sem confirmar que o processo antigo morreu.
* **Compose:** um serviço por node, cada um com seu banco (`evolution_node_01`, …).
* **Entre nodes:** o RelayPlane só migra uma instância depois de provar o fechamento da sessão antiga (`FENCING_FAILED` bloqueia; veja [OPERATIONS](../OPERATIONS.md#migração-e-migration_blocked)).

## Antes de tudo: o banco existe e tem backup

```sql
CREATE DATABASE "evolution-v2-0";   -- um por ordinal, antes de aumentar o StatefulSet
```
O `Secret` precisa de `EVOLUTION_DATABASE_URI_BASE` (URI sem o nome do banco). Faça backup periódico de **cada** banco de node: é ele que contém as sessões.

## Caso 1: o processo caiu ou foi trocado, o banco está intacto (o comum)

Atualização de imagem, `OOMKilled`, nó do cluster que morreu.

1. Confirme o sintoma: `relayplane_provider_node_health == 0` para o node; instâncias dele com `observed_state` `DISCONNECTED`; envios acumulam ou falham com erro retentável.
2. Confirme que o processo antigo **não** está mais rodando (`kubectl get pod -o wide`, `docker ps`).
3. Suba o node de novo **com o mesmo banco** (mesmo nome de pod no StatefulSet; mesmo `DATABASE_CONNECTION_URI` no compose). Não apague o volume nem o banco.
4. Espere: o Evolution sobe, a sessão passa `connecting` → `open` (verificado no número real: sem QR, envio seguinte aceito). O Reconciler promove o node a `READY` na próxima sonda e as instâncias voltam a `CONNECTED`.
5. Verifique:
   ```bash
   curl -H "Authorization: Bearer $ADMIN" "$GW/api/v1/nodes"                       # node READY
   curl -H "Authorization: Bearer $KEY"   "$GW/api/v1/instances/$ID"               # CONNECTED, sem pedido de QR
   curl -H "Authorization: Bearer $KEY"   "$GW/api/v1/messages?status=UNKNOWN"      # envios cortados no meio da queda
   ```
6. **Trate as `UNKNOWN`**: um envio em andamento quando o node caiu pode ter saído ou não. Siga [UNKNOWN-MESSAGES](./UNKNOWN-MESSAGES.md).
7. Eventos recebidos durante a queda: o WhatsApp entrega o que estava pendente quando a sessão volta; o que o aparelho não reenviar não é recuperável. Trate como possível lacuna e compare com o aparelho se for crítico.

O `DISCONNECTED` sem logout também ocorre se o WhatsApp encerrar a sessão (aparelho removido, número banido): isso aparece como `LOGGED_OUT` e **exige QR** (caso 2).

## Caso 2: o banco foi perdido ou a sessão fez logout

As credenciais acabaram: não há como reconectar sem o aparelho.

1. `POST /api/v1/instances/{id}/connect` devolve o pedido de pareamento (QR); o titular escaneia em *Aparelhos conectados*.
2. Mensagens `QUEUED` seguem após a conexão; as `UNKNOWN` seguem o runbook acima.
3. Restaure o banco do backup **antes** de subir o node se quiser tentar o caso 1 (um backup antigo pode estar com sessão já invalidada pelo WhatsApp: nesse caso o QR é inevitável).

## Caso 3: aposentar o node ou mover instâncias

`drain` e `migrate` (veja [OPERATIONS](../OPERATIONS.md#drenaraposentar-um-node)). A migração **sempre** pede QR (a sessão é local ao banco do node antigo) e faz logout lá.

## O que está validado e o que não está

| Verificação | Estado |
|---|---|
| Reposição do processo com o mesmo banco reconecta sem QR, envio seguinte aceito | **verificado** com número real (2026-10-03, [SPIKE-FINDINGS](../SPIKE-FINDINGS.md)) |
| Cada pod do StatefulSet com seu banco; nenhum URI compartilhado | garantido por teste (`internal/archtest`) |
| Envio ambíguo durante a queda vira `UNKNOWN`, retém a fila da instância e se resolve | teste de sistema contra Postgres/Redis/S3 reais |
| Queda e reposição de um node em **staging permanente**, com número | **não feito**: o staging do repositório é efêmero e sem número; repita o caso 1 com um número descartável quando houver ambiente |
