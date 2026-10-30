# Full-Stack Verification Report — Streams for Apache Kafka Proxy on OpenShift

Verified end-to-end on a fresh OpenShift cluster (`apps.ocp.2jdcs.sandbox2809.opentlc.com`), deploying every
step of [openshift-setup.md](openshift-setup.md) with the manifests in `deploy/` and fixing all gaps found.
All work is deployed live; repository changes are **uncommitted** (nothing was committed — say the word if you want a commit).

## Deployment order

The documented deployment order (Guide map in [openshift-setup.md](openshift-setup.md) and
[deploy/README.md](deploy/README.md)) is:

**User-workload monitoring stack (Step 1.1) → Keycloak (Step 2) → Kafka (Step 3) → MirrorMaker 2 (Step 4) → Apicurio Registry (5.1–5.4) → Proxy (Step 6) → HTTP Bridge (7) → Console (8) → Camel clients (9) → Monitoring integration (1.2–1.9) → Custom filter (10) → Demo scenarios (11, incl. the Record Validation demo — Demo 2).**

The user-workload monitoring *stack* (`cluster-monitoring-config` with `enableUserWorkload: true`,
now a ready-to-apply manifest at
[`deploy/observability/00-cluster-monitoring-config.yaml`](deploy/observability/00-cluster-monitoring-config.yaml))
is deployed **first** — it only needs cluster-admin and has no component dependencies, so
enabling it first means every component's metrics are collected from the moment it deploys.
Keycloak follows, then Kafka (whose broker OAuth issuer/JWKS URLs reference the Keycloak
deployment, so having Keycloak up first means brokers validate tokens against a running realm
on their first authentication), then **MirrorMaker 2** — its only dependencies are both Kafka
clusters being Ready and the Cluster Operator watching `kafka-mm2`, so replicating from the
start means the target is in sync from the first record — then the **Apicurio Registry**
(5.1–5.4), which has no dependency on the proxy (the dependency runs the other way: the
proxy's schema-validation filter fetches schemas from the registry), so deploying it first
means the filter resolves schemas from the moment it is wired. The **HTTP Bridge** follows the
proxy — it connects through it (its `bootstrapServers` is the proxy's clusterIP bootstrap), so it
can't reach Kafka before the proxy is up. The **Console** follows the bridge — it also connects
through the proxy (virtual cluster + OIDC against Keycloak). The Record Validation demo moved into **[Demo scenarios](docs/11-demos.md)** (Demo 2) — its manifest now lives
in `deploy/demos/02-record-validation-demo.yaml` and deploys with the optional demo scenarios.
The per-component monitoring
*integration* (netpols, PodMonitors, console metrics source, alerts — 1.2–1.9) necessarily deploys last, after those components exist.

On the verified cluster the stack was originally deployed in guide order (Kafka → Keycloak → … →
Monitoring); the monitoring enablement itself is cluster-level with no dependencies, so the final
state is identical under either order — verified live by the user-workload Prometheus scraping
Keycloak's ServiceMonitor (`up{namespace="kafka-auth", service="keycloak-service"} = 1`).

## Final state — every component Ready and verified

| Step | Component | Namespace | Status | End-to-end verification |
|---|---|---|---|---|
| 3 | Kafka clusters 1 + 2 | `kafka-cluster-1/2` | Ready | `kafka-topics`/CLI against both |
| 2 | Keycloak (RBK) + postgres | `kafka-auth` | Ready | realm import Done; tokens minted and validated |
| 6 | Kroxylicious proxy + operator | `kafka-proxy` | Ready (1 replica) | SASL_SSL produce+consume through the proxy (kafka-cli pod) |
| 7 | HTTP Bridge | `kafka-http-bridge` | Ready | produce (offset 125) and consume of the JSON topic verified over HTTP |
| 8 | Streams Console | `kafka-console` | Running (2/2) | UI serves; OIDC signin endpoint responds |
| 9 | Camel producer + consumer | `kafka-producer`/`kafka-consumer` | Available | consumer receives schema-conformant JSON (`{"orderId":"C-…","amount":…}`) produced through the proxy |
| 5 | Apicurio Registry + postgres | `kafka-registry` | Ready | schema registered (contentId=1); record validation live; Routes + Keycloak auth + role-based authz (anonymous reads; sr-admin token-protected writes) |
| 1 | User-workload monitoring + alerts | `openshift-user-workload-monitoring` | Up | **all** components scraped (below) |
| 4 | MirrorMaker 2 | `kafka-mm2` | Ready | `kafka-cluster-1.test-topic` mirrored to cluster-2 and data verified |

Prometheus targets all `up=1`: brokers (6 pods across both cluster namespaces), proxy, proxy operator,
bridge, console, console operator, Keycloak. Broker metric `kafka_server_replicamanager_underreplicatedpartitions = 0` for all six brokers.

Record validation (Step 5.6): a conforming record (`{"orderId":"A-1","amount":10}`) is delivered;
`this is not json` is rejected by the proxy with `SCHEMA_RESOLUTION_ERROR` before reaching the broker.

## Gaps found on the fresh cluster (all fixed, live + manifests/docs updated)

1. **Cluster Operator crash-loops (403s) — watched namespaces must exist.**
   `deploy/kafka/01-kafka-nodepool.yaml` now includes `Namespace` docs for `kafka-cluster-1`,
   `kafka-cluster-2`, `kafka-http-bridge` (MM2's `kafka-mm2` for Step 4 comes with its own).

2. **CA/secret material is never copied by the operators (missing docs commands).**
   Manual copies required: `kafka-cluster-{1,2}-cluster-ca-cert` → `kafka-proxy`; `proxy-tls` → `proxy-ca`
   Secret (bridge) and `proxy-ca` ConfigMaps (producer/consumer); `keycloak-tls-secret` →
   `keycloak-ingress-ca` ConfigMap (console). `console-kafka-tls` carries inline PEM trust material.

3. **`${OPENSHIFT_APPS_DOMAIN}` empty envsubst pitfall.**
   `deploy/env.sh` must be sourced in the same shell as `envsubst`; an empty var produced
   `keycloak-ingress-kafka-auth.:8080` and forced a broker roll when corrected.

4. **Broker PodMonitor discovered no targets — OpenShift `ignoreNamespaceSelectors: true`.**
   Root-caused with the generated scrape config: OpenShift's user-workload Prometheus sets
   `ignoreNamespaceSelectors: true`, so a PodMonitor's cross-namespace `namespaceSelector` is ignored.
   Fix: **one PodMonitor per broker namespace** (manifest updated) **plus** an `allow-monitoring`
   NetworkPolicy per broker namespace (manifest updated). Broker metrics now scrape `up` on all 6 pods.

5. **MM2 Ready but zero replication — `sourceConnector` placement bug in the manifest.**
   In the v1 `KafkaMirrorMaker2` API, `sourceConnector` lives at **mirror level**, not under
   `spec.mirrors[].source`; nested there it is silently pruned by the CRD and the operator generates
   zero connectors (Connect API returns `[]`). Fixed in `deploy/mirrormaker2/01-mirrormaker2.yaml`;
   replication verified end-to-end afterwards.

6. **Apicurio `registry-postgres-db` Secret is an imperative Step 5.2 prerequisite** — added before the
   registry CR; reconcile re-triggered with `strimzi.io/restart=rolling`.

7. **Image-digest pinning after rebuilds** — the Camel Deployments pin image digests; after each
   `start-build`, re-apply the Deployment manifest to roll the new image (done for both Camel clients).

8. **`kcat` verification commands in the docs lack SASL config** — the docs' record-validation kcat test
   can't fetch OAuth tokens; the kafka-cli pod pattern (with `sasl.login.callback.handler.class` +
   `sasl.oauthbearer.token.endpoint.url` + PEM truststore) is the working verification path.

All of the above are documented as troubleshooting entries in [openshift-setup.md](openshift-setup.md)
(Troubleshooting Tips section) and, where applicable, in the step docs and manifest comments.

## Uncommitted repository changes

- `deploy/kafka/01-kafka-nodepool.yaml` — Namespace docs (operator 403 fix)
- `deploy/kafka/02-kafka.yaml` — issuer templated `${OPENSHIFT_APPS_DOMAIN}`
- `deploy/observability/01-metrics-netpols.yaml` — per-broker-namespace monitoring netpols
- `deploy/observability/02-kafka-podmonitor.yaml` — one PodMonitor per broker namespace
- `deploy/mirrormaker2/01-mirrormaker2.yaml` — `sourceConnector` at mirror level
- `deploy/observability/00-cluster-monitoring-config.yaml` — monitoring-stack enablement manifest (deploy order 1)
- `deploy/apicurio/03-registry.yaml` — registry Routes + Keycloak OIDC auth (anonymous reads, token-protected writes); `04-netpol-dmz.yaml` — router allowance (policy-group label for the hostNetwork router)
- `deploy/keycloak/04-kafka-realm-import.yaml` — registry-api/registry-ui clients for the registry
- `deploy/demos/02-record-validation-demo.yaml` — moved from `deploy/apicurio/05-record-validation-demo.yaml` (Record Validation demo now in Demo scenarios)
- `deploy/camel/producer/src/main/java/org/example/KafkaProducerRoute.java` — producer generates records
  conforming to the record-validation demo JSON schema (`{"orderId": string, "amount": number}`),
  plus a `sent:` log line; rebuilt and redeployed (build 3)
- `deploy/camel/producer|consumer/` — client-side OAuth fixes (handler class + token endpoint set
  programmatically, `jose4j` dependency)
- `openshift-setup.md` — deployment-order Guide map + troubleshooting entries for all gaps above
- `docs/` — step docs updated for the new deployment order and the proxy flattening (steps 6–15 →
  Step 6 with subsections 6.1–6.10, all later steps renumbered 7–11); the Record Validation demo (Demo 2)
  moved from `docs/05-apicurio-registry.md` into `docs/11-demos.md` (Demo 2)

No commit was made; the existing `keycloak` project on the cluster was never touched.
