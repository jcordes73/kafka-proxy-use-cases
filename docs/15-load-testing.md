# Reference: Load testing the data path (Kafka and HTTP clients)

> [↑ Setup overview](../openshift-setup.md) · prev: [Preparing a disconnected (air-gapped) installation](14-disconnected-install.md)

This document is the **measurement plan for the 4,000-TPS and bridge-throughput questions** (Kafka and bridge channels): *"Can the setup support 4,000 TPS on both channels?"* and *"What is the expected throughput for a Kafka bridge?"* Red Hat publishes no benchmark for either channel, so the honest answer is a PoC measurement — and this is the procedure for running it, with ready-to-apply Jobs in [`deploy/loadtest/`](../deploy/README.md). Everything runs **in-cluster** and reads its verdict from the metrics wiring that Step 1 already deployed.

The two channels are **not symmetric** and are tested separately:

| Channel | Tool | Path | Scaling behavior |
|---|---|---|---|
| **Kafka (through the proxy)** | `kafka-producer-perf-test.sh` / `kafka-consumer-perf-test.sh` (ship in the Strimzi Kafka image the docs already use — no new tooling) | load generator → virtual cluster bootstrap → proxy → broker | proxy is stateless — scales with `KafkaProxy` replicas ([Step 6](06-proxy.md)) |
| **HTTP (bridge)** | [k6](https://grafana.com/oss/k6/) scenarios (`deploy/loadtest/03–04`) | HTTP client → bridge Service → proxy → broker | producers stateless (scale out); **consumers pinned per bridge instance** ([7.4](07-http-bridge.md#74--operational-notes)) |

## 15.1 — Method: the rules that make the numbers real

- **Define TPS together with record size.** 4,000 TPS × 1 KiB is ~4 MB/s per channel; 4,000 × 100 KiB is a different system entirely. The manifests default to 1 KiB — set `RECORD_SIZE` (and, for realistic payloads, `RECORDS_PER_REQUEST` on the bridge) to match the actual workload's message profile.
- **Run in-cluster.** A laptop→Route path adds DNS/TLS/ingress hops you don't want in the number. All Jobs here run as pods. If the DMZ front door (gateway/OSSM, [7.7–7.8](07-http-bridge.md#77--concrete-products-for-the-edge-gatewayoauth-proxy)) is part of the production path, run one bridge test through it and one direct — that separates the hop cost.
- **Closed loop for capability, open loop for latency.** The perf-test tools *pace themselves* at `--throughput` and report achieved rate + latency (capability testing). The bridge producer scenario is deliberately **open loop** (`constant-arrival-rate`): an "as fast as possible" HTTP test saturates, then reports garbage latency — a fixed arrival rate plus p95 thresholds is the honest shape ([k6's rationale](https://grafana.com/docs/k6/latest/testing-guides/test-types/), same reason wrk2 exists).
- **Step-rate to find the knee.** One 10-minute steady-state run at 4,000 TPS proves the *target*; the PoC deliverable is the **knee**: repeat at 500 → 1k → 2k → 4k → 8k. Saturation shows in three places at once — latency percentiles inflect, generator/pod CPU pins, and the proxy's backpressure metric (`kroxylicious_*_reads_paused_seconds_sum`) goes non-zero ([Step 1.10](01-user-workload-monitoring.md#110--perses-dashboards-in-the-console-optional-technology-preview) proxy dashboard).
- **Watch the replication side effect.** `topic-A` is MM2-replicated ([Step 4](04-mirrormaker2.md)), so a flood on it is also a flood through MirrorMaker 2 — that is *part of the measured system* (a realistic one). Note it when comparing runs.
- **Quotas are part of the system.** If per-principal quotas were applied ([Step 3.5](03-kafka-cluster.md#35--quotas-per-principal-broker-enforced)), they cap the load user — throttle on a fresh topic with a clean client is the quota test, not a bug.

## 15.2 — The Kafka channel (through the proxy)

Prerequisites (both verified live in [6.10](06-proxy.md#610--apply-and-verify) / [Demo 5](demos/05-stable-endpoints.md)):

```bash
# 1. The proxy CA as a ConfigMap in kafka-proxy (skip if Demo 5 already created it)
oc -n kafka-proxy get secret proxy-tls -o jsonpath='{.data.tls\.crt}' | base64 -d > /tmp/proxy-ca.crt
oc -n kafka-proxy apply -f - <<EOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: proxy-ca
  namespace: kafka-proxy
data:
  ca.crt: |
$(sed 's/^/    /' /tmp/proxy-ca.crt)
EOF
rm -f /tmp/proxy-ca.crt

# 2. Load-test credentials — kafka-user-a via the kafka-console client's password
#    grant (Demo 5's verified pattern; the demo ACLs grant user-a WRITE+DESCRIBE
#    on topic-A and READ on kafka-user-a-group)
oc -n kafka-proxy create secret generic loadtest-oauth \
  --from-literal=username=kafka-user-a --from-literal=password=password-a
```

The default route is the **`kafka-cluster-2` virtual cluster bootstrap** — deliberately not cluster-1's: the base deploy wires the record-validation filter ([Demo 2](demos/02-record-validation.md)) into cluster-1's virtual cluster (topic-scoped to `topic-A`), and the perf tool's synthetic fixed-size payloads are *not* schema-valid — they would be rejected and the run would measure the filter's reject path. cluster-2's virtual cluster carries only sasl-inspection + authorization ([6.5](06-proxy.md#65--define-the-backend-cluster-kafkaservice)). (The bridge tests below *do* send schema-valid payloads, because the bridge produces through cluster-1.)

```bash
# Produce: 1,200,000 × 1 KiB at 4,000 rec/s (10 min steady state) — override per run
oc apply -f deploy/loadtest/01-kafka-producer-perf.yaml
oc -n kafka-proxy wait --for=condition=Complete job/kafka-producer-perf --timeout=1800s
oc -n kafka-proxy logs job/kafka-producer-perf | tail -5

# Consume: drain the same records through the same virtual cluster
oc apply -f deploy/loadtest/02-kafka-consumer-perf.yaml
oc -n kafka-proxy wait --for=condition=Complete job/kafka-consumer-perf --timeout=1800s
oc -n kafka-proxy logs job/kafka-consumer-perf | tail -5
```

Reading the output: the producer prints `{records produced, MB.s, nMB.s, avg latency, max latency}` per reporting interval and a summary at the end — **compare the achieved rate to the target** (a gap means the client itself, the network hop, or a downstream saturated; check generator CPU first) and take the average/percentile latency from the steady-state portion, not the ramp. The consumer reports MB.s and per-interval fetch rates for the drain.

## 15.3 — The HTTP bridge channel

Two Jobs, in the `api-gateway` namespace — the position the bridge's DMZ NetworkPolicy ([7.5](07-http-bridge.md#75--restrict-http-access-to-the-bridge-dmz-networkpolicy)) already admits. The bridge's REST API has no built-in authentication ([7.4](07-http-bridge.md#74--operational-notes)), so no credentials are needed from this side; the bridge's own Kafka-side identity (`kafka-bridge` via OAUTHBEARER through the proxy, [Step 7.2](07-http-bridge.md#72--deploy-the-bridge-with-a-kafkabridge-resource)) is what the brokers see.

**Producer path (scales):** `constant-arrival-rate` POSTs to `/topics/topic-A` with schema-valid records (the record-validation filter is topic-scoped to `topic-A` on cluster-1, and the bridge produces through cluster-1 — synthetic garbage would measure the reject path):

```bash
oc apply -f deploy/loadtest/03-bridge-produce-load.yaml
oc -n api-gateway wait --for=condition=Complete job/bridge-produce-load --timeout=1800s
oc -n api-gateway logs job/bridge-produce-load | tail -20
```

**Consumer path (the ceiling):** creates one bridge consumer (`loadtest-consumer-group`), then long-polls `GET /records` + offset commits in a loop — a throughput measurement, not a paced one. **Run it against a single bridge replica** (the base deploy's `spec.replicas: 1`): consumer state is in-memory per instance, so this number is the per-instance capacity that scaling does *not* spread ([7.4](07-http-bridge.md#74--operational-notes)).

```bash
oc apply -f deploy/loadtest/04-bridge-consume-load.yaml
oc -n api-gateway wait --for=condition=Complete job/bridge-consume-load --timeout=1800s
oc -n api-gateway logs job/bridge-consume-load | tail -20
# Custom counter `bridge_records_consumed` at the bottom of the summary = records/s
```

The producer scenario carries **thresholds** (`http_req_failed < 1%`, `p(95) < 100 ms`) — when the SLO breaks the Job fails, which makes a knee-finding sweep scriptable: change `TARGET_TPS` via `oc set env job/... ` (or a patched copy) and re-run per step. Adjust the p95 threshold to the workload's latency budget before the first run so "pass" means something.

## 15.4 — Reading the results (what lands in the PoC report)

Cross-check every load run against the metrics wiring ([Step 1](01-user-workload-monitoring.md)) — the client-side number is only half the verdict:

| Signal | Where | What it tells you |
|---|---|---|
| `kroxylicious_client_to_proxy_request_total` vs `kroxylicious_proxy_to_server_request_total` | proxy dashboard ([1.10](01-user-workload-monitoring.md#110--perses-dashboards-in-the-console-optional-technology-preview)) | requests entering = leaving (no stall at the proxy) |
| `kroxylicious_*_reads_paused_seconds_sum/_max` | proxy dashboard | **non-zero = a downstream is saturated** — the knee's signature |
| proxy/bridge container CPU (`container_cpu_usage_seconds_total`) | console dashboards | which hop pins first — that hop is the scaling target |
| bridge `strimzi_bridge_kafka_producer_*` / `_consumer_*` | bridge PodMonitor ([1.5](01-user-workload-monitoring.md)) | the bridge's Kafka-client internals under load |
| MM2 replication lag during the run | [Step 4](04-mirrormaker2.md) verification | the flood's side effect on the mirror |

Per data point, record: **target rate, achieved rate, record size, p50/p95/p99 latency, CPU per pod (each hop), backpressure non-zero yes/no** — one row per step-rate level. Two comparisons complete the picture:

- **Filter cost:** re-run the knee sweep with the Record Encryption filter wired (README benchmark note: ~25% lower per-partition throughput at the proxy) — the on/off delta *is* a PoC deliverable. Same method for any filter set change ([6.10](06-proxy.md#610--apply-and-verify) restart note).
- **Channel asymmetry:** report the bridge consumer number per **instance** and the Kafka-channel number per **proxy replica** — they scale differently, and conflating them is the classic mistake this test exists to prevent.

## 15.5 — Air-gapped clusters

Two images in this procedure are outside `registry.redhat.io` — both are in the [disconnected-install](14-disconnected-install.md#step-5--the-directly-referenced-non-red-hat-images) directly-referenced list: `quay.io/strimzi/kafka:latest-kafka-4.2.2` (the perf-test Jobs; the same image the CLI smoke-test pods already use) and `docker.io/grafana/k6` (the bridge Jobs — pin the tag for reproducibility). Mirror both before running the suite in the air-gapped cluster.
