# Versões pinadas

Nenhum componente crítico usa `latest`. Imagens são referenciadas por **tag + digest** (`tag@sha256:…`) em
`docker-compose.yml`, `deploy/docker/compose.infra.yml`, `Dockerfile` e `deploy/kubernetes/`.

| Componente | Versão | Digest |
|---|---|---|
| Gateway / Worker / Reconciler | `0.1.0` (`RELAYPLANE_VERSION`, estampado via `-ldflags -X main.version`) | construído localmente (`relayplane/{gateway,worker,reconciler}:0.1.0`) |
| Evolution API (node) | `v2.3.7` — imagem `evoapicloud/evolution-api` | `sha256:1bd8afc4a6cf48822e6cf02469aeae7bd35a12a6b616eacd1291926307f4d339` |
| Baileys (dentro da Evolution v2.3.7) | `7.0.0-rc.9` (lido de `/evolution/package.json` da imagem) | — |
| Adapter | `evolution/v2` ⇄ Evolution `2.x` (checagem em `ProbeNode`; major ≠ 2 ⇒ node não-READY) | — |
| PostgreSQL | `17.5-alpine` | `sha256:6567bca8d7bc8c82c5922425a0baee57be8402df92bae5eacad5f01ae9544daa` |
| Redis | `7.4.4-alpine` | `sha256:ee9e8748ace004102a267f7b8265dab2c618317df22507b89d16a8add7154273` |
| MinIO | `RELEASE.2025-09-07T16-13-09Z` (`quay.io/minio/minio`) | `sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e` |
| Go (build) | `golang:1.26.5-alpine` | `sha256:0178a641fbb4858c5f1b48e34bdaabe0350a330a1b1149aabd498d0699ff5fb2` |
| Go (testes com `-race`) | `golang:1.26.5` | `sha256:705e964a93a2fd2e75c7d59bb7d781b57e30f12293ffde5175c69229e18fb678` |
| Runtime | `gcr.io/distroless/static-debian12:nonroot` | `sha256:afa5c872c891853ca7fcf1f12c3edb23f7eeef36189728842dd51042ff57f7ab` |
| SDK Python | `relayplane-client 0.1.0` (httpx ≥ 0.27) | — |

Matriz de compatibilidade

| Gateway | Adapter | Evolution | Status |
|---|---|---|---|
| 0.1.x | `evolution-v2` | 2.3.x (testado com 2.3.7) | suportado |
| — | `evolution-v3` | 3.x | não implementado |

Para atualizar um digest: `docker pull <imagem:tag>` → `docker image inspect --format '{{index .RepoDigests 0}}' <imagem:tag>` →
atualizar **todos** os arquivos acima e esta tabela.
