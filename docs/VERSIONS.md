# Versões pinadas

Nenhum componente crítico usa `latest`. Imagens são referenciadas por **tag + digest** (`tag@sha256:…`) em
`docker-compose.yml`, `deploy/docker/compose.infra.yml`, `Dockerfile` e `deploy/kubernetes/`.

| Componente | Versão | Digest |
|---|---|---|
| Gateway / Worker / Reconciler | `0.2.0` (`RELAYPLANE_VERSION`, estampado via `-ldflags -X main.version`) | construído localmente (`relayplane/{gateway,worker,reconciler}:0.2.0`) |
| Evolution API (node) | `v2.3.7`, **reconstruída** em [`deploy/docker/evolution/Dockerfile`](../deploy/docker/evolution/Dockerfile) como `relayplane/evolution:2.3.7-baileys-rc13` | base: `evoapicloud/evolution-api:v2.3.7@sha256:1bd8afc4a6cf48822e6cf02469aeae7bd35a12a6b616eacd1291926307f4d339` (após publicar a imagem no seu registry, fixe o digest dela nos manifests) |
| Baileys (na imagem acima) | **`7.0.0-rc13`** (instalado com versão exata e verificada no build) | — |
| Adapter | `evolution/v2` ⇄ Evolution **exatamente** `2.3.7` (allow-list `v2.TestedVersions`; qualquer outra versão ⇒ node não-READY; ampliável com `EVOLUTION_ALLOWED_VERSIONS` após testes) | — |
| PostgreSQL | `17.5-alpine` | `sha256:6567bca8d7bc8c82c5922425a0baee57be8402df92bae5eacad5f01ae9544daa` |
| Redis | `7.4.4-alpine` | `sha256:ee9e8748ace004102a267f7b8265dab2c618317df22507b89d16a8add7154273` |
| Object store (padrão) RustFS | `1.0.1` (`rustfs/rustfs`) | `sha256:1803faef57627e2d9c2e7d89d655d712ddded5389040054987163043fecb6a3c` |
| Object store SeaweedFS | `4.47` (`chrislusf/seaweedfs`) | `sha256:ce9e796f1fe6f06968f4c04bdaf8f678dad9c8acdfef3d244133d71bfa6bf882` |
| Object store MinIO (legado, opt-in, **imagem não mais baixável**; perfil `minio`) | `RELEASE.2025-09-07T16-13-09Z` (`quay.io/minio/minio`) | `sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e` |
| Go (build) | `golang:1.26.5-alpine` | `sha256:0178a641fbb4858c5f1b48e34bdaabe0350a330a1b1149aabd498d0699ff5fb2` |
| Go (testes com `-race`) | `golang:1.26.5` | `sha256:705e964a93a2fd2e75c7d59bb7d781b57e30f12293ffde5175c69229e18fb678` |
| Runtime | `gcr.io/distroless/static-debian12:nonroot` | `sha256:afa5c872c891853ca7fcf1f12c3edb23f7eeef36189728842dd51042ff57f7ab` |
| SDK Python | `relayplane-client 0.10.0` (httpx ≥ 0.27; a versão fica em `relayplane/_version.py` e o `pyproject.toml` deve concordar: há teste) | — |

> **Segurança (CVE-2026-48063, CVSS 9.3).** A imagem oficial `v2.3.7` embute Baileys `7.0.0-rc.9`, afetado (faixa `>= 7.0.0-rc.1, < 7.0.0-rc12`:
> mensagens/`messages.upsert` forjados, corrupção de app-state e histórico falso). Por isso o RelayPlane **não usa** a imagem oficial: o compose
> e os manifests referenciam a imagem derivada, e `internal/archtest/supply_chain_test.go` falha o build se alguém reintroduzir a imagem oficial,
> um Baileys `< rc12` ou uma tag flutuante. As tags oficiais mais novas foram avaliadas e **não** servem: `2.4.0-rc2` e `latest` ainda trazem `rc.9`;
> `homolog` (2.4.0, Baileys `rc13`) falha ao migrar o banco (Prisma 7 sem `prisma.config` na imagem).

Matriz de compatibilidade

| Gateway | Adapter | Evolution | Status |
|---|---|---|---|
| 0.2.x | `evolution-v2` | 2.3.7 (Baileys 7.0.0-rc13) | suportado |
| 0.2.x | `evolution-v2` | 2.4.x | não testado (recusado por padrão) |
| — | `evolution-v3` | 3.x | não implementado |

Como atualizar o Evolution/Baileys: [runbook PROVIDER-UPGRADE](./runbooks/PROVIDER-UPGRADE.md).

**Fluxo de release da imagem Evolution** (`make evolution-image`): build → confere o Baileys instalado → scan (trivy ou docker scout, se instalados; sem scanner o alvo avisa
que **não** escaneou) → com `REGISTRY=...` faz push e imprime o digest imutável (`registry/evolution@sha256:…`) que deve ir para o compose/manifests. Não implante por tag mutável.

**CI/CD:** `.github/workflows/ci.yml` (a cada PR/push em `main`) e `.github/workflows/evolution-image.yml` (build, scan e publicação numa só ação, `.github/actions/build-scan-push`: o build escaneado é o publicado; manual ou tag `evolution-*`; publica
`ghcr.io/<owner>/relayplane-evolution:<tag>` só depois do scan e imprime o digest no *job summary*). As actions de terceiros são fixadas por SHA de commit,
o que é verificado por `TestWorkflowActionsArePinnedByCommitSHA`; atualize-as resolvendo o novo SHA da tag (`gh api repos/<org>/<repo>/git/ref/tags/<tag>`).

Para atualizar um digest: `docker pull <imagem:tag>` → `docker image inspect --format '{{index .RepoDigests 0}}' <imagem:tag>` →
atualizar **todos** os arquivos acima e esta tabela.
