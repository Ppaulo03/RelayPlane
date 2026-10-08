# Release e staging

Como o RelayPlane vira imagens imutáveis, como essas imagens sobem num ambiente que se parece com produção e o que isso garante
(e não garante).

## O que um release produz

Empurrar uma tag `v<versão>` (ou rodar o workflow `release` manualmente informando a versão) dispara `.github/workflows/release.yml`:

```
version ─ scan ×5 ─────────────────────────────┐   fase 1: cada imagem (gateway, worker, reconciler, simulador, Evolution) é construída UMA vez,
          build único, SCAN desse build,       │           escaneada e guardada como artefato. Sem permissão de escrever pacotes.
          guarda o arquivo OCI                 │
                                               ▼   (só começa se TODOS os scans passaram)
          publish ×5 ──────────────────────────┐   fase 2: envia exatamente os bytes escaneados e confere o digest no registro
                                               ▼
             manifests   cmd/render-release: relayplane-<v>.yaml com TODA imagem fixada por digest + images.env + o sandbox
                 │
          staging-smoke  sobe as imagens publicadas em modo produção e roda o smoke
```

* **Um release é publicado inteiro ou não é publicado.** O job `scan` (uma imagem por vez, em matriz) **não tem permissão de escrever pacotes**: constrói a imagem uma vez, para um arquivo OCI (com proveniência e SBOM), roda o Trivy
  (gravidade `CRITICAL`, só o que tem correção) **nesse arquivo** e o guarda como artefato com o digest escaneado. O job `publish` só começa quando **todas** as imagens passaram (`needs: scan` espera a matriz inteira); ele baixa o arquivo, confere
  que o digest é o escaneado, faz o `crane push` **desses mesmos bytes** e confere que o digest que o registro devolve é o digest que foi escaneado. Se o scan de uma imagem reprova, **nada é publicado** (o `0.3.0-rc4` mostrou o contrário no
  desenho antigo: o Evolution reprovou depois de as outras quatro imagens já estarem no registro). As duas fases vivem em ações compostas (`.github/actions/build-scan` e `.github/actions/publish`); `internal/archtest` falha se
  algum workflow voltar a construir para dar `push`, se o job de scan ganhar permissão de escrita, se o `publish` deixar de depender do `scan` ou se um componente deixar de passar pelas duas fases.
  A política é `ignore-unfixed: true` (um CRITICAL sem correção disponível não bloqueia; é uma escolha de risco consciente) e o scan olha o sistema operacional e as dependências da imagem, não a sua configuração.
  Resta o caso de uma falha **durante** a publicação (o registro fora do ar no meio): os pushes são verificados um a um, mas uma interrupção ali ainda pode deixar um release parcial.
* **Ensaio sem publicar:** `gh workflow run release.yml -f version=<v> -f publish=false` (pela execução manual) constrói e escaneia todas as imagens e **não publica nada**: responde "este commit pode virar release?".
* **Nenhum placeholder chega a um manifesto.** `deploy/kubernetes/relayplane.yaml` é um *template* (`registry.example.com/relayplane/<componente>:…@sha256:REPLACE…`); o
  `render-release` o transforma em `relayplane-<v>.yaml` e **recusa** produzir algo com placeholder, tag flutuante, digest curto, componente sem imagem ou imagem desconhecida
  (`internal/release`, testado contra o template real do repositório).
* **O sandbox sem o repositório** (`relayplane-sandbox-<v>.tar.gz`: script, compose e as imagens deste release, mais a do simulador) sai junto; veja [SANDBOX](./SANDBOX.md#sem-clonar-o-repositório).
* **O resultado fica no release do GitHub** (quando veio de tag) e no artefato do run: `relayplane-<v>.yaml` (Kubernetes) e `images.env` (as mesmas referências para o staging em compose).
  O resumo do run lista os digests.

Pacotes do GHCR: neste repositório (público) os pacotes publicados são públicos (verificado com `docker manifest inspect` sem login no `0.3.0-rc1`), então qualquer host puxa as imagens sem segredo. Se o repositório for privado, os pacotes nascem privados: torne-os públicos ou dê ao cluster/host um segredo de pull. Em *Settings → Actions → General* o `GITHUB_TOKEN` precisa de permissão
de escrita em pacotes.

```bash
git tag v0.1.0 && git push origin v0.1.0
```

## Staging: a configuração de produção, a partir das imagens do release

```bash
sh deploy/staging/staging.sh init                  # segredos em .env.staging (fora do git), APP_ENV=production
sh deploy/staging/staging.sh up images.env         # o images.env do release; recusa qualquer imagem que não seja repositório@sha256:…
sh deploy/staging/staging.sh smoke
sh deploy/staging/staging.sh images                # que digest cada contêiner realmente roda
sh deploy/staging/staging.sh down -v
```

`deploy/staging/compose.staging.yml` é um override do `docker-compose.yml`: **nada é construído** (`build: !reset null`), as quatro imagens vêm de variáveis **sem valor padrão**, e
`APP_ENV=production` liga as guardas de produção (webhooks só `https` e destinos públicos, segredos de 16+ caracteres, sem os `WEBHOOKS_ALLOW_*` de desenvolvimento).
Para ensaiar localmente com o código atual: `sh deploy/staging/staging.sh build-local` constrói as imagens e escreve um `images.env` marcado como local (única forma de subir sem digest).

### O que o smoke prova

Com o nó **Evolution real** (sem número de WhatsApp):
* o gateway está *ready* (Postgres, Redis, fila de comandos e object store responderam) e expõe métricas;
* os dois nós Evolution ficam `READY` **na versão com que o adapter foi validado (2.3.7)**;
* as guardas de produção estão ligadas: webhook `http://`, `127.0.0.1`, `10.x` e `169.254.169.254` são recusados;
* a chave de API revogada para na hora e a última chave não se revoga;
* uma instância nova pede pareamento e o nó real devolve um **QR code**.

O mesmo smoke roda **em todo PR** (job `staging` do CI, com as imagens construídas dos mesmos Dockerfiles) e **no release**, com as imagens exatamente como foram publicadas.

### O que o smoke não prova

Parear um celular e o comportamento das mensagens (isso é o [`REAL-NUMBER-SPIKE`](./REAL-NUMBER-SPIKE.md)), carga, e nada sobre a **sua** infraestrutura: o repositório não provisiona
host, cluster, DNS, TLS, gestão de segredos nem backup. O staging do CI é efêmero. Para um staging permanente você precisa de um host ou cluster e dos segredos; com isso, os comandos acima
(compose) ou `kubectl apply -f relayplane-<v>.yaml` (Kubernetes, com o ConfigMap e o Secret descritos no manifesto) são o caminho.

## Um defeito que só a medição de latência mostrou

Ao medir a latência de entrega apareceu um defeito antigo do dispatcher: o `UPDATE` que arrenda uma entrega só rechecava o **lease**. Com vários workers, se outro acabasse de entregar a mesma
entrega (ou de agendar um retry), o PostgreSQL relia a linha, via o lease vazio e **arrendava de novo**: eventos já entregues eram reenviados (5 a 330 reenvios em 2000 eventos, conforme a carga) e o
backoff dos retries era ignorado. O `UPDATE` agora rechecka o estado inteiro (`PENDING`, `next_attempt_at`, lease), e há testes de concorrência contra o PostgreSQL real (que reproduziram o defeito: 136 a 205
de 300 entregas arrendadas mais de uma vez) para a fila de entregas e a de anexos. Depois da correção o sink recebe **exatamente** uma requisição por evento.

## Rollback

Reaplique o manifesto (ou o `images.env`) do release anterior. As migrações do banco são **só para frente** (todas aditivas até hoje): voltar de imagem não desfaz uma migração, então um
release que traga migração destrutiva precisa de plano próprio. Os nós Evolution guardam a sessão no banco deles: trocar a imagem não pede novo QR.

## A imagem Evolution: o que o scan encontrou

O workflow da imagem Evolution nunca tinha rodado, e o primeiro scan local mostrou que a imagem oficial tem achados **CRITICAL com correção**, que o `push` teria barrado:

| Achado | Onde | Correção no `deploy/docker/evolution/Dockerfile` |
|---|---|---|
| CVE-2026-31789 | `openssl` do Alpine | `apk add 'openssl>=3.5.6-r0' …` |
| CVE-2026-25896 | `fast-xml-parser` (3 cópias: Evolution, cliente minio, SDK da AWS) | `overrides`: 5.3.5 em todo lugar, 4.5.4 dentro do minio |
| CVE-2026-59873 | `tar` do próprio `npm` (o entrypoint roda `npm run start:prod`, então o npm fica) | `npm@11.21.0` |
| CVE-2025-68121 | binário `esbuild` do `tsx` (ferramenta de desenvolvimento; produção roda `node dist/main`) | removido |

**Uma armadilha que quase passou:** a primeira versão desse ajuste usava `npm install --no-save` em passos separados, e o segundo passo **reverteu o Baileys para a versão vulnerável**
(instalações `--no-save` são podadas pela instalação seguinte). Agora tudo é **uma instalação gravada no `package.json`** e a verificação (Baileys e todas as cópias do `fast-xml-parser`) roda
**no fim do build, sobre a árvore final**.

Ainda há achados `HIGH` na própria árvore de dependências do Evolution (axios, brace-expansion…) que este repositório não corrige sem fazer um fork; o workflow os lista num passo
informativo (não bloqueia) para nunca serem surpresa. O que bloqueia é `CRITICAL` com correção.

## Para o time de operação

* **`/metrics` está na porta pública do gateway**, sem autenticação. Bloqueie `/metrics` no ingress ou publique só a porta de operação (os workers e o reconciler já usam `OPS_PORT`).
* O limite de requisições por tenant é **por réplica do gateway**.
* Postgres é a fonte da verdade: backup e restauração são seus. Redis guarda comandos e eventos em trânsito (`appendonly` ligado no compose).
