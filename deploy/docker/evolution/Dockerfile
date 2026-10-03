# Evolution API v2.3.7 with Baileys upgraded to a release that fixes CVE-2026-48063
# (message spoofing via placeholderResendMessage; fixed in 7.0.0-rc12 / 6.7.22).
# The base image is pinned by digest; the Baileys version is pinned exactly and verified at build time.
FROM evoapicloud/evolution-api:v2.3.7@sha256:1bd8afc4a6cf48822e6cf02469aeae7bd35a12a6b616eacd1291926307f4d339
ARG BAILEYS_VERSION=7.0.0-rc13
ENV TZ=UTC
WORKDIR /evolution
RUN npm install --no-save --no-audit --no-fund baileys@${BAILEYS_VERSION} \
 && node -e "const v=require('baileys/package.json').version; if (v!=='${BAILEYS_VERSION}') { console.error('unexpected baileys '+v); process.exit(1) } console.log('baileys '+v)"
