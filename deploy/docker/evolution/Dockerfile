# Evolution API v2.3.7 with Baileys upgraded to a release that fixes CVE-2026-48063
# (message spoofing via placeholderResendMessage; fixed in 7.0.0-rc12 / 6.7.22), and with the CRITICAL findings of the stock image
# fixed (found by the release scan, see docs/RELEASE.md). The base image is pinned by digest; every version below is pinned
# exactly and verified at the END of the build, after every installation step (an `npm install --no-save` is pruned by the next
# install: the verification must look at the final tree, never at an intermediate one).
FROM evoapicloud/evolution-api:v2.3.7@sha256:1bd8afc4a6cf48822e6cf02469aeae7bd35a12a6b616eacd1291926307f4d339
ARG BAILEYS_VERSION=7.0.0-rc13
ENV TZ=UTC
WORKDIR /evolution

# ONE installation, recorded in package.json (so nothing later removes it):
#  * baileys >= 7.0.0-rc12 (CVE-2026-48063)
#  * fast-xml-parser (CVE-2026-25896, fixed in 5.3.5 / 4.5.4): 5.3.5 everywhere, except inside the minio client, which is on the 4.x line
#  * proxy-addr (CVE-2026-90711, fixed in 2.0.8): a transitive dependency of express that the release scan of 0.3.0-rc4 found CRITICAL
RUN npm pkg set "dependencies.baileys=${BAILEYS_VERSION}" "overrides.fast-xml-parser=5.3.5" "overrides.minio.fast-xml-parser=4.5.4" "overrides.proxy-addr=2.0.8" \
 && npm install --no-audit --no-fund

# Other CRITICAL findings of the stock image that have a fix:
#  * openssl (CVE-2026-31789, fixed in 3.5.6-r0)
#  * npm's own tar (CVE-2026-59873): the entrypoint runs `npm run start:prod`, so npm stays, updated
#  * the esbuild binary of tsx (Go stdlib CVE-2025-68121): tsx is a development tool, `start:prod` is `node dist/main`
RUN apk add --no-cache 'openssl>=3.5.6-r0' 'libcrypto3>=3.5.6-r0' 'libssl3>=3.5.6-r0' \
 && npm install -g --no-audit --no-fund npm@11.21.0 \
 && rm -rf /evolution/node_modules/tsx/node_modules/@esbuild /root/.npm /tmp/*

# Evolution awaits a profile-picture query (no timeout of its own) for every message it handles, in a sequential loop: when WhatsApp does not
# answer, messages show up 60 s apart (the Baileys query timeout). RelayPlane does not use pictures: off by default, bounded when enabled.
COPY patch-profile-picture.js /tmp/patch-profile-picture.js
RUN node /tmp/patch-profile-picture.js && rm /tmp/patch-profile-picture.js

# the final tree is what ships: check it
RUN node -e " \
  const cp=require('child_process'); \
  const baileys=require('/evolution/node_modules/baileys/package.json').version; \
  if (baileys!=='${BAILEYS_VERSION}') { console.error('baileys is '+baileys+', expected ${BAILEYS_VERSION}'); process.exit(1) } \
  const tree=JSON.parse(cp.execSync('npm ls fast-xml-parser --all --json',{cwd:'/evolution',maxBuffer:1<<26}).toString()); \
  const found=[]; (function walk(n,path){ for (const [k,v] of Object.entries(n.dependencies||{})) { if (k==='fast-xml-parser') found.push([path.concat(k).join('>'),v.version]); walk(v,path.concat(k)) } })(tree,[]); \
  const bad=found.filter(([p,v])=>!(p.startsWith('minio>')?v==='4.5.4':v==='5.3.5')); \
  if (!found.length||bad.length) { console.error('fast-xml-parser: '+JSON.stringify(found)); process.exit(1) } \
  const pa=[]; const pt=JSON.parse(cp.execSync('npm ls proxy-addr --all --json',{cwd:'/evolution',maxBuffer:1<<26}).toString()); \
  (function walk(n,path){ for (const [k,v] of Object.entries(n.dependencies||{})) { if (k==='proxy-addr') pa.push([path.concat(k).join('>'),v.version]); walk(v,path.concat(k)) } })(pt,[]); \
  if (!pa.length||pa.some(([p,v])=>v!=='2.0.8')) { console.error('proxy-addr: '+JSON.stringify(pa)); process.exit(1) } \
  console.log('baileys '+baileys+'; fast-xml-parser '+JSON.stringify(found)); \
  require('/evolution/node_modules/minio'); \
  if (!require('fs').readFileSync('/evolution/dist/main.js','utf8').includes('RELAYPLANE_PROFILE_PICTURE_TIMEOUT_MS')) { console.error('profile picture patch missing'); process.exit(1) } \
"
