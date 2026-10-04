"""Gate on the delivery lag measured by the dispatchers themselves.

    docker run ... curl http://worker:9090/metrics | python tools/loadcheck/lag.py 2s

Reads Prometheus text (one or more scrapes concatenated) and checks relayplane_event_delivery_lag_seconds{attempt="first"}: the
upper bound of the bucket that holds the 95th percentile must not exceed the limit. This is measured with the clock of the database
and the workers, so it does not depend on the clock of the machine that runs the test (a consumer on another host would add its own skew).
"""
from __future__ import annotations

import re
import sys

LIMIT = sys.argv[1] if len(sys.argv) > 1 else "2s"
limit = float(LIMIT[:-2]) / 1000 if LIMIT.endswith("ms") else float(LIMIT.rstrip("s"))

buckets: dict[float, float] = {}
count = 0.0
pat = re.compile(r'^relayplane_event_delivery_lag_seconds_(bucket|count)\{([^}]*)\}\s+([0-9.eE+-]+)$')
for line in sys.stdin:
    m = pat.match(line.strip())
    if not m or 'attempt="first"' not in m.group(2):
        continue
    value = float(m.group(3))
    if m.group(1) == "count":
        count += value
        continue
    le = re.search(r'le="([^"]+)"', m.group(2)).group(1)
    bound = float("inf") if le == "+Inf" else float(le)
    buckets[bound] = buckets.get(bound, 0.0) + value

if count == 0:
    print("no first-attempt delivery lag samples were scraped")
    sys.exit(2)
need = 0.95 * count
p95 = next(b for b in sorted(buckets) if buckets[b] >= need)
print(f"delivery lag measured by the dispatchers: {int(count)} first-attempt events, p95 <= {p95}s (limit {limit}s)")
if p95 > limit:
    print(f"FAIL: p95 delivery lag is above {limit}s")
    sys.exit(1)
