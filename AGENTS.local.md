# AGENTS.local.md — RelayPlane

Deltas específicos deste repositório (o `AGENTS.md` global continua valendo).

## Stack e comandos

* Go 1.26 (módulo `github.com/relayplane/relayplane`), PostgreSQL 17, Redis Streams 7.4, object store S3-compatível (RustFS padrão; SeaweedFS e MinIO suportados), Evolution API v2.3.7.
* `make test` (unit+contrato+sistema em memória) · `make test-integration` (infra real em `deploy/docker/compose.infra.yml`,
  portas 55440/56390/59010) · `make test-race` (Docker) · `make sdk-test`.
* Stack completa: `cp .env.example .env && docker compose up -d --build`.
* Testes de integração usam a build tag `integration`; suíte de sistema real: `RELAYPLANE_SYSTEMTEST_BACKEND=real`.
* Regra de arquitetura verificada por `internal/archtest`: core só stdlib; ports só core; app/worker/reconciler/api sem adapters;
  nada da Evolution fora de `adapters/providers/evolution`. Quebrou o teste = quebrou a arquitetura.

## Aprendizados acumulados

* **Baileys/Evolution (CVE-2026-48063):** a imagem oficial `evoapicloud/evolution-api:v2.3.7` traz Baileys `7.0.0-rc.9` (vulnerável). Usamos
  `deploy/docker/evolution/Dockerfile` (base v2.3.7 + `npm install baileys@7.0.0-rc13`; o Baileys é `require`d externamente, então a troca em
  `node_modules` funciona). Tags oficiais mais novas: `2.4.0-rc2`/`latest` ainda `rc.9`; `homolog` (rc13) quebra em `prisma migrate deploy`.
  `internal/archtest/supply_chain_test.go` guarda isso. Reavalie ao sair uma release oficial 2.4.x estável.
* **Locks de lifecycle:** toda mutação de instância roda em `Deps.WithInstanceControl` (lock `instance-control:<id>`); quem já segura o lock
  chama `ProvisionLocked`/`FinishDeleteLocked`. Durante migração ativa o reconciler não age (por `operations`, não por `observed_state`).
* **Ordenação:** `sequence_no` por instância é alocado no mesmo INSERT (lock da linha de `instances`); comando bloqueado por predecessor em voo
  volta ao outbox (ack + `ResetOutbox`) em vez de esperar no lugar, senão o predecessor republicado fica *atrás* na mesma chave (deadlock).
  O corte de tempo do `UNKNOWN` usa o relógio do banco (skew entre containers e host quebrou a versão com `time.Time`).

* **Race detector no Windows local:** não há gcc ⇒ `go test -race` falha (`requires cgo`). Rodar em container (`make test-race`).
* **Vazão de envio = partições, não workers** (`COMMAND_PARTITIONS`, medido com `make test-load-stack`: 1, 3 e 12 workers entregam o mesmo ~170-200 msg/s a 32 partições;
  8 → 83, 128 → 325). `docker kill` não aciona a restart policy do compose (o script de caos faz `docker start` depois). O `loadgen` mede entrega pelos contadores do stub,
  não pelo polling de status (polling serial de milhares de mensagens fingia um teto).
* **Redis sem persistência:** após restart o consumer group some e `XREADGROUP` devolve `NOGROUP`; o adapter recria o grupo (queue e bus). Descoberto por
  `make test-chaos` (usa `docker compose pause/restart` nos containers de `deploy/docker/compose.infra.yml`; tmpfs do Postgres é perdido em stop/start, por isso ele só é pausado).
* **Object store:** `minio/minio` saiu do Docker Hub (community edition em manutenção reduzida). O adapter S3 é genérico; `make test-integration-s3`
  roda contrato + suíte de sistema em RustFS, SeaweedFS e MinIO. SeaweedFS: cada bucket é uma *collection* que pré-aloca volumes (30 GB por padrão):
  o teste cria um bucket por teste, então a infra de teste usa `-master.volumeSizeLimitMB=32 -volume.max=2000`; sem isso → "internal error".
  `docker compose down` não remove serviços de perfis inativos: use `docker compose --profile "*" down -v` ao trocar de backend.
  minio-go trunca em silêncio um corpo maior que o tamanho declarado → `Put` do adapter usa `exactReader`.
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
