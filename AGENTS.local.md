# AGENTS.local.md — RelayPlane

Deltas específicos deste repositório (o `AGENTS.md` global continua valendo).

## Stack e comandos

* Go 1.26 (módulo `github.com/relayplane/relayplane`), PostgreSQL 17, Redis Streams 7.4, MinIO/S3, Evolution API v2.3.7.
* `make test` (unit+contrato+sistema em memória) · `make test-integration` (infra real em `deploy/docker/compose.infra.yml`,
  portas 55440/56390/59010) · `make test-race` (Docker) · `make sdk-test`.
* Stack completa: `cp .env.example .env && docker compose up -d --build`.
* Testes de integração usam a build tag `integration`; suíte de sistema real: `RELAYPLANE_SYSTEMTEST_BACKEND=real`.
* Regra de arquitetura verificada por `internal/archtest`: core só stdlib; ports só core; app/worker/reconciler/api sem adapters;
  nada da Evolution fora de `adapters/providers/evolution`. Quebrou o teste = quebrou a arquitetura.

## Aprendizados acumulados

* **Race detector no Windows local:** não há gcc ⇒ `go test -race` falha (`requires cgo`). Rodar em container (`make test-race`).
* **MinIO:** `minio/minio` saiu do Docker Hub; usar `quay.io/minio/minio:<RELEASE…>` (pinar digest).
* **Shell:** em Git Bash, comandos muito longos com vários heredocs `'EOF'` podem falhar com "unexpected EOF"; criar arquivos
  com a ferramenta Write em vez de heredocs gigantes. `docker run -v` precisa de `MSYS_NO_PATHCONV=1` e `pwd -W`.
* **Evolution v2.3.7:** `GET /instance/connectionState` não distingue "nunca pareado" de "desconectado" (ambos `connecting`/`close`);
  o adapter consulta `fetchInstances?instanceName=` e usa `ownerJid`. Criar instância repetida devolve **403** "already in use".
  Não existe "fechar socket sem logout": fencing físico = `logout` (⇒ nova leitura de QR após migrar).
* **Postgres `FOR UPDATE` + `WHERE active_instances < capacity`:** a re-avaliação do WHERE após o lock é o que impede dois creates de
  pegarem o último slot; manter a ordem `ORDER BY id` ao travar vários nodes (evita deadlock).
* **Relógio do Windows:** resolução baixa; testes que comparam "heartbeat mais antigo que 0" devem usar timeout negativo.
* **Idempotência:** só libera a chave (`Abandon`) para erros de rejeição pré-efeito (`invalid/forbidden/not_found/no_capacity/…`);
  erros transitórios mantêm o claim para que o retry reuse o mesmo resource id (senão cria recurso duplicado).
* **Webhook de `connection.update`:** `ProviderMessageID` = timestamp do evento, senão dois `CONNECTED` legítimos colapsam no dedupe.
