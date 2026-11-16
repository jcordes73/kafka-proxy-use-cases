#!/usr/bin/env bash
# make-guide-pdf.sh — build a single merged PDF from the repo's linked markdown.
#
# Order: README (overview) → openshift-setup.md (the setup guide) → docs/01–12
# (component guides, incl. the demo index + walkthrough) → docs/demos/01–14.
# Cross-file markdown links are rewritten to internal PDF links (each file's
# first H1 becomes the link target when no anchor is given). Mermaid blocks are
# rendered to vector PDF via mermaid-filter (needs @mermaid-js/mermaid-cli and
# mermaid-filter, installed globally: npm install -g @mermaid-js/mermaid-cli mermaid-filter).
# Emoji are mapped to LaTeX-safe symbols on a temp copy; README.md and docs/ are
# never modified.
set -euo pipefail
cd "$(dirname "$0")"

OUT="${1:-kafka-proxy-guide.pdf}"
WORK=$(mktemp -d /tmp/opencode/guide-pdf.XXXXXX)
trap 'rm -rf "$WORK"' EXIT

FILES=(README.md openshift-setup.md docs/01-user-workload-monitoring.md docs/02-keycloak.md \
  docs/03-kafka-cluster.md docs/04-mirrormaker2.md docs/05-apicurio-registry.md docs/06-proxy.md \
  docs/07-http-bridge.md docs/08-console.md docs/09-camel-clients.md docs/10-demos.md \
  docs/11-demos-walkthrough.md docs/12-cleanup.md docs/14-disconnected-install.md \
  docs/15-load-testing.md docs/demos/01-token-revocation.md \
  docs/demos/02-record-validation.md docs/demos/03-live-migration.md docs/demos/04-dmz-pivot.md \
  docs/demos/05-stable-endpoints.md docs/demos/06-proxy-authorization.md \
  docs/demos/07-entity-isolation.md docs/demos/08-token-validation.md \
  docs/demos/09-multi-tenant-virtual-clusters.md docs/demos/10-mm2-failover.md \
  docs/demos/11-record-encryption.md docs/demos/12-schema-evolution.md \
  docs/demos/14-disaster-recovery.md)

python3 - "$WORK" "${FILES[@]}" <<'EOF'
import re, sys, os, unicodedata

work = sys.argv[1]
files = sys.argv[2:]

def gh_slug(h):
    s = unicodedata.normalize('NFKD', h.strip())
    s = s.replace('&', '')
    s = re.sub(r'[^\w\s-]', '', s, flags=re.UNICODE)
    return re.sub(r'\s', '-', s).lower().strip('-')

# first H1 of each included file -> its GitHub-style id (pandoc gfm_auto_identifiers
# generates the same ids; duplicates get -N suffixes, first occurrence keeps the bare slug)
h1 = {}
for f in files:
    for line in open(f, encoding='utf-8'):
        m = re.match(r'^#\s+(?!#)(.*)$', line)
        if m:
            h1[os.path.normpath(f)] = gh_slug(m.group(1))
            break

EMOJI = {'\ufe0f': '', '✅': '\u2714', '❌': '\u2718', '🔐': '', '🔒': '', '🎬': '', '📋': '', '🧭': '', '\U0001F9EA': ''}

def rewrite(m):
    path, anchor = m.group(1), m.group(2) or ''
    if path.startswith('http'):
        return m.group(0)
    target = os.path.normpath(os.path.join(os.path.dirname(cur), path))
    if target not in h1:          # not part of the merged doc (e.g. deploy/README.md)
        return m.group(0)
    return '](#' + (anchor[1:] if anchor else h1[target]) + ')'

parts = []
for cur in files:
    t = open(cur, encoding='utf-8').read()
    for k, v in EMOJI.items():
        t = t.replace(k, v)
    t = re.sub(r'\]\(([^)#\s]+\.md)(#[^)\s]*)?\)', rewrite, t)
    parts.append(t)

os.makedirs(work, exist_ok=True)
open(os.path.join(work, 'guide.md'), 'w', encoding='utf-8').write('\n\n'.join(parts))
print(f'merged {len(files)} files -> {work}/guide.md')
EOF

cat > "$WORK/h1pagebreak.lua" <<'EOF'
-- insert \newpage before every top-level heading (except the very first)
local first = true
function Header(el)
  if el.level == 1 then
    if first then first = false; return el end
    return {pandoc.RawBlock('latex', '\\newpage'), el}
  end
  return el
end
EOF

export MERMAID_FILTER_FORMAT=pdf MERMAID_FILTER_SHOW=none MERMAID_FILTER_BACKGROUND=white
pandoc "$WORK/guide.md" -f gfm -o "$OUT" \
  --from=gfm --lua-filter="$WORK/h1pagebreak.lua" --filter=mermaid-filter \
  --pdf-engine=xelatex --toc --toc-depth=2 -V toc-title="Contents" \
  -V title="Streams for Apache Kafka Proxy on OpenShift — Use Cases, Setup Guide and Demos" \
  -V mainfont="DejaVu Sans" -V monofont="Liberation Mono" -V geometry:margin=2cm \
  --resource-path=.:docs:docs/demos

echo "wrote $OUT ($(pdfinfo "$OUT" 2>/dev/null | awk '/^Pages/{print $2}') pages)"
