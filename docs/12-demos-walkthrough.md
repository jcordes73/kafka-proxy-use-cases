# Step 12: End-to-end demo walkthrough

> [↑ Setup overview](../openshift-setup.md) · prev: [Step 11: Demo scenarios](11-demos.md)

The [Demo scenarios](11-demos.md) index holds the 13 scenarios as standalone documents under [`docs/demos/`](demos). This document strings them into **one presentation** — a single arc from "one door" to "brokers can't read the data", in an order that builds, with talking points, rough timing, and the revert between acts. Per-demo detail, prerequisites, and gotchas stay in each demo's document; each section below links back to it. Demo numbers throughout refer to the **original demo numbers** (the document file names), not the renumbered tables.

All commands run from the **repository root** (as in the demo scenarios doc), from a machine that resolves in-cluster DNS for the Keycloak/proxy services.

## The arc

| Act | Demos | Story | ~Time |
|---|---|---|---|
| 1 — The door | [4](#act-1--the-door-demos-4-5-1), [5, 1](#act-1--the-door-demos-4-5-1) | the proxy is the single Kafka-protocol door; endpoints never move; identity lives in Keycloak | 10 min |
| 2 — Contracts without code | [2, 12](#act-2--contracts-without-code-demos-2-12) | data quality enforced **at the proxy** — no client change, no broker config | 10 min |
| 3 — Move without touching clients | [3, 10, 14](#act-3--move-without-touching-clients-demos-3-10-14) | mirror, cutover, failover, DR — clients ride through on the same bootstrap | 10 min |
| 4 — Enforcement at the edge | [6, 7, 8](#act-4--enforcement-at-the-edge-demos-6-7-8) | authorization, isolation, and token checks **before** the brokers | 10 min |
| 5 — The finale | [11](#act-5--the-finale-encryption-at-rest-demo-11) | Record Encryption at rest — the brokers hold ciphertext | 10 min |
| Coda (optional) | [9](#coda--optional-extensions-demo-9) | multi-tenant virtual clusters | 10 min |

**Total: 45–60 minutes** with the optional coda. Acts 1, 2, and 5 alone make a tight 30-minute version.

## Act 0 — Before you present

The stack must be fully deployed ([steps 1–9](../openshift-setup.md#guide-map) — components table in the [Guide map](../openshift-setup.md#guide-map)). Five minutes of prep saves every act:

```bash
# 1. Environment (cluster domain for the Keycloak admin route)
source deploy/env.sh    # or: export OPENSHIFT_APPS_DOMAIN=<apps-domain>

# 2. The proxy is Ready, and the baseline wiring is in place
oc get virtualkafkacluster -n kafka-proxy          # kafka-cluster-1, kafka-cluster-2 — Ready

# 3. Get the demo-client token once — reused in Acts 1 and 4
ACCESS_TOKEN=$(curl -s http://keycloak-http.kafka-auth.svc.cluster.local:8080/realms/kafka/protocol/openid-connect/token \
  -d "client_id=demo-client" -d "client_secret=demo-client-secret" -d "grant_type=client_credentials" | jq -r .access_token)

# 4. Extract the SASL subject SaslInspection will derive — Demo 6 needs it
echo ${ACCESS_TOKEN} | jq -R 'split(".") | .[1] | @base64d | fromjson | .sub'   # a UUID — keep it visible

# 5. The schema contentId for Demo 2 (from Step 5.4 of the registry guide)
export APICURIO_CONTENT_ID=1
```

Pre-presentation checklist:

- **Keycloak admin password** at hand (Demo 1's revocation) — or drive the disable in the Console UI, which demos better.
- **Dashboards open** — user-workload monitoring ([Step 1](01-user-workload-monitoring.md)) with the Kafka, proxy (`kroxylicious_*`), and Camel client panels. Act 1 and the closing read from them.
- **Baseline revert state** — the untouched manifests [`deploy/proxy/05-virtualkafkacluster.yaml`](../deploy/proxy/05-virtualkafkacluster.yaml) and [`deploy/mirrormaker2/01-mirrormaker2.yaml`](../deploy/mirrormaker2/01-mirrormaker2.yaml). Every revert in this walkthrough is re-applying one of these (full checklist at the [end](#revert-checklist)).
- **demo-client enabled** in Keycloak (it is the demo workhorse — Act 1 disables it, and everything after needs it back).

## Act 1 — The door (Demos 4, 5, 1)

**The message:** *the proxy is the single Kafka-protocol door, what's behind it can change freely, and identity is centralized.*

### Demo 4 — the DMZ pivot attempt ([detail](demos/04-dmz-pivot.md))

Open with the negative result: brokers are unreachable from anywhere but the proxy namespace.

```bash
# From an unrelated namespace: brokers are unreachable — TIMEOUT
oc run kcat-blocked --rm -i --restart=Never --image=confluentinc/cp-kcat:latest -n default -- \
  -b kafka-cluster-1-kafka-bootstrap.kafka-cluster-1.svc.cluster.local:9092 -L

# From kafka-proxy: the only way in is through the proxy
oc run kcat-allowed --rm -i --restart=Never --image=confluentinc/cp-kcat:latest -n kafka-proxy -- \
  -b kafka-cluster-1-kafka-bootstrap.kafka-cluster-1.svc.cluster.local:9092 -L
```

> Talking point: *treat proxy access as broker access* — the backend NetworkPolicies ([Step 3.4](03-kafka-cluster.md#34--restrict-kafka-ingress-to-the-proxy-namespace-dmz)) make the proxy the only door. Everything that follows passes through it.

### Demo 5 — stable endpoints ([detail](demos/05-stable-endpoints.md))

Roll every broker of the upstream cluster; clients don't notice.

```bash
oc rollout restart deployment -n kafka-cluster-1 || oc delete pod -l strimzi.io/cluster=kafka-cluster-1 -n kafka-cluster-1
oc logs -f deployment/camel-kafka-producer -n kafka-producer   # keeps producing, no errors
```

> Talking point: clients bootstrap to the proxy's clusterIP service, never to broker addresses — broker topology changes are invisible.

### Demo 1 — token revocation, the OAuth payoff ([detail](demos/01-token-revocation.md))

The centerpiece of the act: disable the client in Keycloak, watch access stop at the next token expiry — zero changes to the stack.

```bash
KEYCLOAK_HOST="keycloak-ingress-kafka-auth.${OPENSHIFT_APPS_DOMAIN}"
ADMIN_TOKEN=$(curl -s "https://${KEYCLOAK_HOST}/realms/master/protocol/openid-connect/token" \
  -d "client_id=admin-cli" -d "username=admin" -d "password=<admin-password>" -d "grant_type=password" | jq -r .access_token)
CLIENT_UUID=$(curl -s -H "Authorization: Bearer ${ADMIN_TOKEN}" \
  "https://${KEYCLOAK_HOST}/admin/realms/kafka/clients?clientId=demo-client" | jq -r '.[0].id')
curl -X PUT -H "Authorization: Bearer ${ADMIN_TOKEN}" -H "Content-Type: application/json" \
  -d '{"enabled": false}' "https://${KEYCLOAK_HOST}/admin/realms/kafka/clients/${CLIENT_UUID}"
```

The Kafka CLI pod keeps working until its cached token expires (~5 min — `accessTokenLifespan: 300` in the realm import), then fails. **Re-enable the client immediately after** (`"enabled": true`) — it is the workhorse for the rest of the walkthrough; the 5-minute expiry window is your talking point, not your schedule.

> No infrastructure changes in this act — nothing to revert.

## Act 2 — Contracts without code (Demos 2, 12)

**The message:** *data quality is enforced at the proxy — the contract changed once, centrally; clients and brokers unchanged.*

### Demo 2 — Record Validation end-to-end ([detail](demos/02-record-validation.md))

```bash
envsubst < deploy/demos/02-record-validation-demo.yaml | oc apply -f -
# deploy/proxy/05-virtualkafkacluster.yaml already wires filterRefs [record-validation]
# on kafka-cluster-1 — the patch below is idempotent:
oc -n kafka-proxy patch virtualkafkacluster kafka-cluster-1 --type=merge \
  -p '{"spec":{"filterRefs":[{"name":"record-validation"}]}}'
oc -n kafka-proxy rollout status deploy/simple --timeout=180s   # proxy reconfigures
```

Then the money shot — a conforming record is delivered, a non-conforming one is rejected **before the broker**:

```bash
echo '{"orderId":"A-1","amount":10}' | oc run kcat-valid --rm -i --restart=Never \
  --image=confluentinc/cp-kcat:latest -- \
  -b kafka-cluster-1-cluster-ip-1-bootstrap.kafka-proxy.svc.cluster.local:9292 -t topic-A -P -l
# delivered — exit 0

echo 'this is not json' | oc run kcat-invalid --rm -i --restart=Never \
  --image=confluentinc/cp-kcat:latest -- \
  -b kafka-cluster-1-cluster-ip-1-bootstrap.kafka-proxy.svc.cluster.local:9292 -t topic-A -P -l
# % Delivery failed ... Broker failed to validate record
```

> ⚠️ **A malformed filter crash-loops the proxy** (unknown config fields → `CrashLoopBackOff` until corrected). Two verified gotchas in the [Demo 2 notes](demos/02-record-validation.md): `allowNulls`/`allowEmpty` sit inside the rule definition, and the guide's `wireFormatVersion` does not exist in the deployed proxy — use the ready-made manifest. This is also the demo to rehearse most thoroughly.

> Talking point while the proxy reconfigures: no client was restarted, no broker was touched — a filter was attached to a virtual cluster's data path.

### Demo 12 — schema evolution ([detail](demos/12-schema-evolution.md))

Same demo, one layer up: change the JSON schema in Apicurio Registry (UI or REST — e.g. make `amount` required), re-produce the previously-valid record — now rejected. The contract changed **once, centrally**; no client redeployment, no broker config.

**Revert after the act:** remove the filter wiring and optionally the `KafkaProtocolFilter` — re-apply [`deploy/proxy/05-virtualkafkacluster.yaml`](../deploy/proxy/05-virtualkafkacluster.yaml) (idempotent) plus `oc -n kafka-proxy patch virtualkafkacluster kafka-cluster-1 --type=json --path=/spec/filterRefs --value=[]` first ([Demo 2, "to disable"](demos/02-record-validation.md)).

## Act 3 — Move without touching clients (Demos 3, 10, 14)

**The message:** *the data path is virtual — the physical backend can move underneath it.* Run these in order: plain cutover → offset-translated failover → full DR.

### Demo 3 — live migration / cutover ([detail](demos/03-live-migration.md))

1. Produce to `topic-A` via the proxy — lands on `kafka-cluster-1`; watch it mirror to cluster-2 ([Step 4.2](04-mirrormaker2.md#42--verify-replication)).
2. **Cutover** — one change, in one place:

   ```bash
   oc -n kafka-proxy patch virtualkafkacluster kafka-cluster-1 --type=merge \
     -p '{"spec":{"targetKafkaServiceRef":{"name":"kafka-cluster-2"}}}'
   ```

   Clients keep using the same virtual-cluster bootstrap — no client change.
3. **Revert now** (before Demo 10 replaces the MM2 CR): re-apply [`deploy/proxy/05-virtualkafkacluster.yaml`](../deploy/proxy/05-virtualkafkacluster.yaml).

### Demo 10 — MM2 failover with offset translation ([detail](demos/10-mm2-failover.md))

```bash
oc apply -f deploy/demos/10-mm2-failover.yaml    # + checkpointConnector (sync.group.offsets) + heartbeatConnector
oc wait --for=condition=Ready kafkamirrormaker2/kafka-cluster-1-to-2 -n kafka-mm2 --timeout=600s
```

Produce with the Camel consumer active (`groupId=camel-consumer`), show the translated offsets (`kafka-cluster-1.camel-consumer` in `mm2-checkpoints.internal` / the console), then repeat Demo 3's cutover — the consumer **resumes at the translated offset on the target**, not from scratch.

**Revert after:** `oc apply -f deploy/mirrormaker2/01-mirrormaker2.yaml` and the proxy baseline.

### Demo 14 — disaster recovery active/passive ([detail](demos/14-disaster-recovery.md))

The same machinery, framed as DR: "lose" the source (`oc scale kafka kafka-cluster-1 -n kafka-cluster-1 --replicas=0` — dev only), switch the upstream to cluster-2 (Demo 3's patch), clients ride through on the same bootstrap with consumers resuming at translated offsets. Recover: scale back, cut back — the rollback is switching the upstream again.

> Talking point: across all of Act 3, the client configuration never changed — the virtual cluster is the stability point, the physical cluster is a detail.

## Act 4 — Enforcement at the edge (Demos 6, 7, 8)

**The message:** *checks run before requests reach the brokers — the proxy knows who the client is.* Each demo **replaces** the virtual cluster's filter wiring; **revert between demos** with `oc apply -f deploy/proxy/05-virtualkafkacluster.yaml`.

### Demo 6 — authorization at the proxy ([detail](demos/06-proxy-authorization.md))

1. Put the Act 0 subject into [`deploy/demos/06-authorization-filters.yaml`](../deploy/demos/06-authorization-filters.yaml) (replace `<DEMO-CLIENT-SUBJECT>`), then apply and wait:

   ```bash
   oc apply -f deploy/demos/06-authorization-filters.yaml
   oc wait --for=condition=Accepted=True virtualkafkacluster/kafka-cluster-1 -n kafka-proxy --timeout=120s
   ```

2. Allow path: demo-client produces to `topic-A` — works. Deny path: `camel-producer`, or demo-client on any other topic — `TOPIC_AUTHORIZATION_FAILED` **before the brokers**.
3. **Revert.**

> Talking point: `SaslInspection` derives the end-client principal from the SASL/OAUTHBEARER exchange (the token's `sub` claim) — the proxy enforces per-principal ACLs even though the brokers have no authorization configured. The filter adds checks *in addition to* the broker's own; both must allow.

### Demo 7 — entity isolation ([detail](demos/07-entity-isolation.md))

```bash
oc apply -f deploy/demos/07-entity-isolation.yaml
```

Two consumers through the proxy with the **same** group name but different OAuth clients (`demo-client`, `camel-producer` tokens) — on the brokers the groups materialize as `<sub-UUID>_demo-consumer-group`, two distinct groups with independent offsets (verified live). The `separator` in the manifest must be `_` — the principal is the JWT `sub` UUID, so `-` fails every request and `~` is rejected at startup. Neither consumer sees or interferes with the other's offsets. **Revert.**

> Framing: a shared cluster without tenant coordination — isolation falls out of identity. Preview caveat: `GROUP_ID` and `TRANSACTIONAL_ID` are isolated, topic names are not yet.

### Demo 8 — token validation on the Kafka cluster ([detail](demos/08-token-validation.md))

Token validation happens **only at the Kafka cluster level**: the brokers' `oauth` listener validates each JWT against Keycloak's JWKS during the SASL/OAUTHBEARER exchange. The proxy relays the exchange unchanged (SASL Passthrough) — it does not participate in validation.

Allow path: a valid demo-client token authenticates and produces normally — the broker validates it against Keycloak's JWKS as part of the SASL exchange. Deny path: forge a token signed with a different key or tamper with one — the **broker** rejects it with a SASL error (e.g. `{"status":"invalid_token"}`) before any produce happens (Kafka CLI command in the [detail](demos/08-token-validation.md)).

> The demo JWKS/issuer URLs use the internal http Keycloak service; for production use the TLS route and mount its CA into the broker pods ([Demo 8 note](demos/08-token-validation.md)).

## Act 5 — The finale: encryption at rest (Demo 11)

**The message:** *the brokers hold ciphertext; only the proxy holds keys.* The wow-factor closer — build it up with [Demo 11's detail](demos/11-record-encryption.md).

```bash
oc apply -f deploy/demos/11-record-encryption.yaml
oc wait --for=condition=Ready pod -l app=vault -n kafka-vault --timeout=120s
oc wait --for=condition=Complete job/kek-setup -n kafka-vault --timeout=180s
oc logs job/kek-setup -n kafka-vault    # KEK-topic-A created
oc wait --for=condition=Accepted=True virtualkafkacluster/kafka-cluster-1 -n kafka-proxy --timeout=120s
```

1. Produce through the proxy (Demo 1 client) — the producer sees **plaintext**; it has no keys and no knowledge of the encryption.
2. Consume **directly from the broker** (bypassing the proxy — Demo 4's rules) — **ciphertext** (the full command is in [Demo 11](demos/11-record-encryption.md)).
3. Consume through the proxy again — **plaintext**. Only the proxy holds decryption capability.
4. **Revert:** re-apply [`deploy/proxy/05-virtualkafkacluster.yaml`](../deploy/proxy/05-virtualkafkacluster.yaml), then `oc delete namespace kafka-vault`.

> ⚠️ Vault runs in **dev mode** (in-memory, fixed root token) — DEV ONLY. Say it out loud in the demo: production is a real Vault with the Transit engine, a least-privilege periodic token, and TLS.

## Coda — optional extensions (Demo 9)

If time allows, one short closer that widens the story:

- **Demo 9 — multi-tenant virtual clusters** ([detail](demos/09-multi-tenant-virtual-clusters.md)): the stack already exposes two virtual clusters; the demo adds a third over the *same* physical backend (`shared-cluster-1`) — logical clusters decoupled from physical ones, each with its own `filterRefs`. Revert: `oc delete virtualkafkacluster/shared-cluster-1 -n kafka-proxy`.

## Closing — read it off the dashboards

End where Act 1 started: the user-workload monitoring dashboards ([Step 1](01-user-workload-monitoring.md)) tell the whole story with no extra tooling — broker and client throughput through the roll (Act 1), the rejected produce that never reached the upstream (`kroxylicious_client_to_proxy_request_total` vs `kroxylicious_proxy_to_server_request_total`, Act 2), the proxy's per-filter activity (Act 4), and the [stack alerts](../deploy/observability/05-kafka-stack-alerts.yaml) watching it all. If the optional **Perses dashboards** are deployed ([Step 1.10](01-user-workload-monitoring.md#110--perses-dashboards-in-the-console-optional-technology-preview)), the `strimzi-kafka`, `strimzi-kraft`, and `strimzi-kafka-bridge` dashboards in the console (`Observe → Dashboards (Perses)`, `kafka` project) show those same signals as panels. The one-sentence version:

> stable endpoints → OAuth-secured data path → validation and contract enforcement at the proxy → mirror + cutover with clients unaware → authorization, isolation, and token checks at the edge → encryption at rest the brokers can't read — all visible in the dashboards.

## Revert checklist

Run top-to-bottom to return the stack to its deployed baseline (the state after [steps 1–9](../openshift-setup.md#guide-map)):

```bash
# Virtual cluster + filter wiring (Acts 2, 4, 5)
oc apply -f deploy/proxy/05-virtualkafkacluster.yaml

# MM2 variant (Act 3) — restore the plain replication CR
oc apply -f deploy/mirrormaker2/01-mirrormaker2.yaml

# Multi-tenant extra cluster (Coda), if applied
oc delete virtualkafkacluster/shared-cluster-1 -n kafka-proxy --ignore-not-found

# Vault dev namespace (Act 5)
oc delete namespace kafka-vault --ignore-not-found

# Keycloak demo-client must be ENABLED (Act 1 reverts it — verify, don't assume)
#   Console UI, or the admin REST PUT with {"enabled": true} from Demo 1

# Then verify the proxy is Ready and clients flow again
oc get virtualkafkacluster -n kafka-proxy
oc -n kafka-proxy rollout status deploy/simple --timeout=180s
```

> If the proxy is in `CrashLoopBackOff` at any point, a demo `KafkaProtocolFilter` with malformed config is still wired — correct or delete the filter resource, then re-apply the baseline wiring ([Demo 2 warning](demos/02-record-validation.md)).
