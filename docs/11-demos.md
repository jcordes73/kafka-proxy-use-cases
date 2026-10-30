# Step 11: Demo Scenarios

> [↑ Setup overview](../openshift-setup.md) · prev: [Step 10: Custom filter](10-custom-filter.md)

Ready-made walkthroughs for demonstrating the stack. Scenarios 1, 3–5 and 12–14 need **no new infrastructure** — they exercise what is already deployed (see the [steps 6–20](../openshift-setup.md)). Scenarios 6–8 and 11 apply a demo manifest from [`deploy/demos/`](../deploy/README.md) that **replaces** the `kafka-cluster-1` `VirtualKafkaCluster` wiring — revert each with `oc apply -f deploy/proxy/05-virtualkafkacluster.yaml`. Scenario 2 applies a manifest that **adds** a demo topic and a Record Validation filter (revert by removing the `filterRefs` entry).

All commands run from the **repository root**.

| # | Scenario | Needs |
|---|---|---|
| 1 | Token revocation (OAuth payoff) | — |
| 2 | Record Validation end-to-end | `deploy/demos/02-record-validation-demo.yaml` |
| 3 | Live migration / cutover (MM2) | — |
| 4 | DMZ pivot attempt | — |
| 5 | Stable endpoints | — |
| 6 | Authorization at the proxy | `deploy/demos/06-authorization-filters.yaml` |
| 7 | Entity isolation (preview) | `deploy/demos/07-entity-isolation.yaml` |
| 8 | Token validation at the proxy | `deploy/demos/08-token-validation-filters.yaml` |
| 9 | Multi-tenant virtual clusters | `deploy/demos/09-multi-tenant-virtual-clusters.yaml` (optional) |
| 10 | MM2 failover / failback | `deploy/demos/10-mm2-failover.yaml` |
| 11 | Record Encryption at rest | `deploy/demos/11-record-encryption.yaml` (Vault dev) |
| 12 | Schema evolution | registry from Step 5.4 |
| 13 | Custom filter | docs [Step 10](10-custom-filter.md) |
| 14 | Disaster recovery active/passive | — |

## Demo 1 — Token revocation (the OAuth payoff)

Show that identity is centralized in Keycloak: access stops at the next token expiry when a client is disabled — no credential rotation anywhere in the stack.

```bash
# 1. Get a token for demo-client and produce through the proxy
ACCESS_TOKEN=$(curl -s http://keycloak-http.kafka-auth.svc.cluster.local:8080/realms/kafka/protocol/openid-connect/token \
  -d "client_id=demo-client" -d "client_secret=demo-client-secret" -d "grant_type=client_credentials" | jq -r .access_token)

# 2. Disable the demo-client in Keycloak (admin REST — or use the console UI)
KEYCLOAK_HOST="keycloak-ingress-kafka-auth.${OPENSHIFT_APPS_DOMAIN}"
ADMIN_TOKEN=$(curl -s "https://${KEYCLOAK_HOST}/realms/master/protocol/openid-connect/token" \
  -d "client_id=admin-cli" -d "username=admin" -d "password=<admin-password>" -d "grant_type=password" | jq -r .access_token)
CLIENT_UUID=$(curl -s -H "Authorization: Bearer ${ADMIN_TOKEN}" \
  "https://${KEYCLOAK_HOST}/admin/realms/kafka/clients?clientId=demo-client" | jq -r '.[0].id')
curl -X PUT -H "Authorization: Bearer ${ADMIN_TOKEN}" -H "Content-Type: application/json" \
  -d '{"enabled": false}' "https://${KEYCLOAK_HOST}/admin/realms/kafka/clients/${CLIENT_UUID}"
```

The already-issued token stays valid for its lifetime (5 min — `accessTokenLifespan: 300` in the realm import). The Kafka CLI pod from [Step 6.10](06-proxy.md#610--apply-and-verify) keeps working until its cached token expires, then fails — **access stops at the next token expiry with zero changes to the stack**. Re-enable the client to restore access.

## Demo 2 — Record Validation end-to-end

> **Deployment order:** the demo topic, filter, and wiring deploy **after the proxy** (they wire `filterRefs` into the proxy's virtual cluster); with the demo scenarios at Step 11 that prerequisite is always satisfied. The schema (`APICURIO_CONTENT_ID`) comes from [Step 5.4](05-apicurio-registry.md#54--register-a-schema).

[`deploy/demos/02-record-validation-demo.yaml`](../deploy/demos/02-record-validation-demo.yaml) declares the demo topic and the `KafkaProtocolFilter` — a `RecordValidation` rule scoped to `record-validation-demo` that checks record **values** against the registered schema (`apicurioId` = the contentId from Step 5.4). `allowNulls`/`allowEmpty` sit **inside** the rule definition (as siblings of `schemaValidationConfig`), and there is no `defaultRule` — all other topics pass through unvalidated:

```bash
export APICURIO_CONTENT_ID=1
envsubst < deploy/demos/02-record-validation-demo.yaml | oc apply -f -
# deploy/proxy/05-virtualkafkacluster.yaml already wires filterRefs
# [record-validation] on kafka-cluster-1 — the patch below is idempotent:
oc -n kafka-proxy patch virtualkafkacluster kafka-cluster-1 --type=merge \
  -p '{"spec":{"filterRefs":[{"name":"record-validation"}]}}'
oc -n kafka-proxy rollout status deploy/simple --timeout=180s   # proxy reconfigures
```

> ⚠️ **Config shape matters — a malformed filter crash-loops the proxy.** The deployed upstream 0.25.1 proxy rejects unknown filter config fields at startup (the pod enters `CrashLoopBackOff` until the `KafkaProtocolFilter` is corrected). Two gotchas verified live: `allowNulls`/`allowEmpty` belong inside the rule definition, and the `wireFormatVersion` option shown in the Red Hat 3.2 guide does not exist in 0.25.1 — omit it.

Produce through the proxy with `kcat` — a conforming record is delivered, a non-conforming one is rejected before the broker:

```bash
echo '{"orderId":"A-1","amount":10}' | oc run kcat-valid --rm -i --restart=Never \
  --image=confluentinc/cp-kcat:latest -- \
  -b kafka-cluster-1-cluster-ip-1-bootstrap.kafka-proxy.svc.cluster.local:9292 \
  -t record-validation-demo -P -l
# (delivered — exit 0)

echo 'this is not json' | oc run kcat-invalid --rm -i --restart=Never \
  --image=confluentinc/cp-kcat:latest -- \
  -b kafka-cluster-1-cluster-ip-1-bootstrap.kafka-proxy.svc.cluster.local:9292 \
  -t record-validation-demo -P -l
# % Delivery failed for message: Broker: Broker failed to validate record
```

Watch `kroxylicious_client_to_proxy_request_total` vs `kroxylicious_proxy_to_server_request_total` in user-workload monitoring — the rejected produce never reaches the upstream (downstream count permanently ahead).

> **To disable validation:** remove the `filterRefs` entry (`oc -n kafka-proxy patch virtualkafkacluster kafka-cluster-1 --type=json --path=/spec/filterRefs --value=[]` and re-apply [`deploy/proxy/05-virtualkafkacluster.yaml`](../deploy/proxy/05-virtualkafkacluster.yaml) to restore the manifest state), and optionally delete the `KafkaProtocolFilter`. Note the 0.25.1 proxy exports no Record Validation–specific metrics — rejections are only visible client-side and in the proxy logs.

## Demo 3 — Live migration / cutover (MM2)

The migration pattern from use case [7 in the guide](../README.md#7-cluster-migration--upstream-switching):

1. Produce to `test-topic` via the proxy (Demo 1 client) — lands on `kafka-cluster-1`.
2. Watch it mirror: `kafka-cluster-1.test-topic` appears on cluster-2 ([Step 4.2](04-mirrormaker2.md#42--verify-replication)).
3. Cutover — one change, in one place (repoint the virtual cluster's `KafkaService`):

   ```bash
   oc -n kafka-proxy patch virtualkafkacluster kafka-cluster-1 --type=merge \
     -p '{"spec":{"targetKafkaServiceRef":{"name":"kafka-cluster-2"}}}'
   ```

   Clients keep using the same virtual-cluster bootstrap — no client change.
4. Rollback is re-applying the manifest wiring ([`deploy/proxy/05-virtualkafkacluster.yaml`](../deploy/proxy/05-virtualkafkacluster.yaml)) or patching `targetKafkaServiceRef` back to `kafka-cluster-1`.

## Demo 4 — DMZ pivot attempt

From [Step 3.4](03-kafka-cluster.md#34--restrict-kafka-ingress-to-the-proxy-namespace-dmz):

```bash
# From an unrelated namespace: brokers are unreachable — TIMEOUT
oc run kcat-blocked --rm -i --restart=Never --image=confluentinc/cp-kcat:latest -n default -- \
  -b kafka-cluster-1-kafka-bootstrap.kafka-cluster-1.svc.cluster.local:9092 -L

# From kafka-proxy: the only way in is through the proxy
oc run kcat-allowed --rm -i --restart=Never --image=confluentinc/cp-kcat:latest -n kafka-proxy -- \
  -b kafka-cluster-1-kafka-bootstrap.kafka-cluster-1.svc.cluster.local:9092 -L
```

The message: *treat proxy access as broker access* — the DMZ makes the proxy the single door.

## Demo 5 — Stable endpoints

Clients never see broker topology changes:

```bash
# Roll every broker of the cluster the proxy targets
oc rollout restart deployment -n kafka-cluster-1 || oc delete pod -l strimzi.io/cluster=kafka-cluster-1 -n kafka-cluster-1

# The Camel producer (timer-driven) keeps producing through the proxy with no
# errors during the roll — its bootstrap is the proxy's clusterIP bootstrap
oc logs -f deployment/camel-kafka-producer -n kafka-producer
```

## Demo 6 — Authorization at the proxy

Enforce per-principal ACLs **before requests reach the brokers**. Requires the authenticated subject: `SaslInspection` extracts the end-client principal from the SASL/OAUTHBEARER exchange.

1. Discover the principal SaslInspection derives for the demo-client — the default subject builder uses the **SASL authorized ID**, which for OAUTHBEARER is the token's `sub` claim (a UUID for Keycloak service accounts):

   ```bash
   echo ${ACCESS_TOKEN} | jq -R 'split(".") | .[1] | @base64d | fromjson | .sub'
   ```

2. Put it in the ACL rules — replace `<DEMO-CLIENT-SUBJECT>` in [`deploy/demos/06-authorization-filters.yaml`](../deploy/demos/06-authorization-filters.yaml) (also grants `demo-*` groups; `otherwise deny` denies everything else), then:

   ```bash
   oc apply -f deploy/demos/06-authorization-filters.yaml
   oc wait --for=condition=Accepted=True virtualkafkacluster/kafka-cluster-1 -n kafka-proxy --timeout=120s
   ```

3. The allow path: demo-client produces to `test-topic` — works.
4. The deny path: `camel-producer` (or the demo-client on any other topic) is rejected with `TOPIC_AUTHORIZATION_FAILED` **before the brokers** — the request never leaves the proxy.
5. Revert: `oc apply -f deploy/proxy/05-virtualkafkacluster.yaml`

> The Authorization filter adds checks *in addition to* the broker's own; both must allow. The brokers currently have no authorization configured, so the proxy's decision is the effective one.

## Demo 7 — Entity isolation (preview)

Each authenticated principal gets a **private namespace for consumer groups and transactional IDs** within the shared cluster — no coordination needed, no collisions possible.

1. Apply the demo manifest (SaslInspection + EntityIsolation):

   ```bash
   oc apply -f deploy/demos/07-entity-isolation.yaml
   ```

2. Run **two** consumers through the proxy using the *same* group name (e.g. `demo-consumer-group`) but different OAuth clients (`demo-client` and `camel-producer` tokens).
3. On the brokers, the groups materialize as `<principal>-demo-consumer-group` — two distinct groups. List them from inside the cluster (bypassing the proxy) to show both exist.
4. Neither consumer sees or interferes with the other's offsets. `GROUP_ID` and `TRANSACTIONAL_ID` are isolated; topic names are not yet ([kroxylicious#3504](https://github.com/kroxylicious/kroxylicious/issues/3504)).
5. Revert: `oc apply -f deploy/proxy/05-virtualkafkacluster.yaml`

## Demo 8 — Token validation at the proxy

Validate JWTs **before** they reach the brokers — invalid, expired, or foreign-issuer tokens stop at the proxy:

1. Apply the demo manifest (SaslInspection + OauthBearerValidation against Keycloak's JWKS):

   ```bash
   oc apply -f deploy/demos/08-token-validation-filters.yaml
   ```

2. The allow path: a valid demo-client token produces normally (the brokers validate it *again* — defense in depth).
3. The deny path: forge a token signed with a different key, or tamper with one — the proxy rejects it with a SASL error before any broker sees it.
4. Revert: `oc apply -f deploy/proxy/05-virtualkafkacluster.yaml`

> The demo JWKS/issuer URLs use the internal http Keycloak service — the same endpoint the brokers use. For production, use the Keycloak TLS route and mount its CA into the proxy pods via the `KafkaProxy` pod template.

## Demo 9 — Multi-tenant virtual clusters

One proxy, many logical clusters — each with its own bootstrap, filters, and policies:

- The stack already exposes **two** virtual clusters over two physical clusters ([deploy/proxy/05](../deploy/proxy/05-virtualkafkacluster.yaml)): `kafka-cluster-1` and `kafka-cluster-2`. Point two clients at the two bootstraps — same proxy, two logical clusters.
- [`deploy/demos/09-multi-tenant-virtual-clusters.yaml`](../deploy/demos/09-multi-tenant-virtual-clusters.yaml) adds a **third** virtual cluster over the *same* physical backend as cluster-1 (`shared-cluster-1`) — showing logical clusters are decoupled from physical ones. Each carries its own `filterRefs`, so per-tenant policies (Demo 6 rules) differ per virtual cluster.
- Revert: `oc delete virtualkafkacluster/shared-cluster-1 -n kafka-proxy`

## Demo 10 — MM2 failover / failback

The offset-translation variant of the replication:

1. Apply the failover variant (adds `checkpointConnector` with `sync.group.offsets.enabled: "true"` and `heartbeatConnector`):

   ```bash
   oc apply -f deploy/demos/10-mm2-failover.yaml
   oc wait --for=condition=Ready kafkamirrormaker2/kafka-cluster-1-to-2 -n kafka-mm2 --timeout=600s
   ```

2. Produce with a consumer group active on the source (the Camel consumer, `groupId=camel-consumer`).
3. Check the translated offsets on the target: `kafka-cluster-1.camel-consumer` in the `mm2-checkpoints.internal` topic / the console.
4. **Failover**: switch the `KafkaService` upstream (Demo 3, step 7) — the consumer resumes at the translated offset on the target, not from scratch.
5. **Failback**: switch the upstream back; reverse replication can be added by mirroring in the other direction.
6. Revert: `oc apply -f deploy/mirrormaker2/01-mirrormaker2.yaml`

## Demo 11 — Record Encryption at rest

Envelope encryption at the proxy with a dev-mode Vault as KMS — **the wow-factor demo**:

1. Apply the demo manifest (Vault dev + Transit KEKs + RecordEncryption filter):

   ```bash
   oc apply -f deploy/demos/11-record-encryption.yaml
   oc wait --for=condition=Ready pod -l app=vault -n kafka-vault --timeout=120s
   oc wait --for=condition=Complete job/kek-setup -n kafka-vault --timeout=180s
   oc logs job/kek-setup -n kafka-vault    # KEK-test-topic created
   oc wait --for=condition=Accepted=True virtualkafkacluster/kafka-cluster-1 -n kafka-proxy --timeout=120s
   ```

2. Produce a message through the proxy (Demo 1 client) — the producer sees plaintext; it has **no keys and no knowledge** of the encryption.
3. Consume **directly from the broker** (bypassing the proxy — from `kafka-proxy` or the cluster's namespace, Demo 4 rules) — **ciphertext**:

   ```bash
   oc run kafka-cli --rm -i --restart=Never --image=quay.io/strimzi/kafka:latest-kafka-4.2.2 -n kafka-proxy -- \
     bash -c 'echo "Y2xpZW50cw==" >/dev/null; bin/kafka-console-consumer.sh \
       --bootstrap-server kafka-cluster-1-kafka-bootstrap.kafka-cluster-1.svc.cluster.local:9092 \
       --topic test-topic --from-beginning --timeout-ms 10000 \
       --consumer-property security.protocol=PLAINTEXT'
   # -> ciphertext records (kroxylicious envelope-encrypted values)
   ```

4. Consume through the proxy again — plaintext: only the proxy holds decryption capability.
5. Topics **without** a KEK (`unresolvedKeyPolicy: PASSTHROUGH_UNENCRYPTED`) pass through unencrypted; switch to `REJECT` so unencrypted data is never forwarded.
6. Revert: `oc apply -f deploy/proxy/05-virtualkafkacluster.yaml`, then `oc delete namespace kafka-vault`

> Vault runs in **dev mode** (in-memory, fixed root token) — DEV ONLY. Production: a real Vault with the Transit engine, a least-privilege periodic token, and TLS ([Red Hat 3.2 Record Encryption guide](https://docs.redhat.com/en/documentation/red_hat_streams_for_apache_kafka/3.2/html/deploying_and_managing_streams_for_apache_kafka_on_openshift)).

## Demo 12 — Schema evolution

Extends Demo 2:

1. Produce a valid record (accepted).
2. Change the JSON schema in Apicurio Registry (UI or REST — e.g. make a field required).
3. Produce the same record again — now rejected with `Broker failed to validate record`.
4. The contract changed **once, centrally** — no client redeployment, no broker config.

## Demo 13 — Custom filter

Write, build, and deploy a custom filter in the data path — the full extension story ([Step 10](10-custom-filter.md)):

1. Generate a filter project with the upstream Maven archetype, implement a small transformation (e.g. inject a `demo-filter` header, or mask a field).
2. Build in-cluster (`deploy/custom-filter/01-buildconfig.yaml`), wire a `KafkaProtocolFilter` ([`deploy/custom-filter/02-kafkaprotocolfilter.yaml`](../deploy/custom-filter/02-kafkaprotocolfilter.yaml)) into the virtual cluster's `filterRefs`.
3. Show the effect through the proxy; show the filter's absence when producing directly to the broker.

## Demo 14 — Disaster recovery active/passive

cluster-2 as the passive replica:

1. MM2 keeps cluster-2 in sync (Demo 3); with Demo 10's `checkpointConnector`, consumer offsets are translated too.
2. "Lose" the source: `oc scale kafka kafka-cluster-1 -n kafka-cluster-1 --replicas=0` (dev only).
3. Switch the proxy upstream to cluster-2 (`KafkaService`, Demo 3 step 7) — clients ride through on the same virtual-cluster bootstrap; consumers resume at the translated offsets (Demo 10).
4. "Recover": scale cluster-1 back, reverse-mirror, and cut back — the rollback is switching the upstream again.

## The combined narrative

> (5) stable endpoints → (1) OAuth-secured data path → (1) revoke a client live → (2/12) validation/contract enforcement at the proxy → (3/14) mirror + cutover with clients unaware → (6/7/8) authorization, isolation, and token checks at the edge → (11) encryption at rest the brokers can't read → all of it visible in (15) the dashboards ([Step 1](01-user-workload-monitoring.md)).
