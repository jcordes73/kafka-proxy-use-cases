# Step 3: Deploy the Kafka Cluster (Streams for Apache Kafka)

> [↑ Setup overview](../openshift-setup.md) · prev: [Step 2: Keycloak (kafka-auth)](02-keycloak.md) · next: [Step 4: MirrorMaker 2](04-mirrormaker2.md)

If you don't already have a Kafka cluster, deploy one using **Streams for Apache Kafka** (Strimzi) from the same `streams-3.2-ocp-install-examples.zip` archive. The example below deploys a small **development** cluster; production notes follow.

> **Deployment order:** in this guide the Kafka cluster is deployed **third** — after the user-workload monitoring stack (Step 1.1) and Keycloak (Step 2) — and MirrorMaker 2 ([Step 4](04-mirrormaker2.md)) follows immediately, replicating from these clusters as soon as they are up; see the Guide map in the [setup overview](../openshift-setup.md). The broker OAuth issuer/JWKS URLs reference the Keycloak deployment, so having Keycloak up first means the brokers validate tokens against a running realm on their first authentication.

## 3.1 — Install the Cluster Operator

1. The Cluster Operator manifests are in `install/cluster-operator/`. Choose a namespace for the Kafka cluster (example: `kafka`) and edit the manifests to match:

   ```bash
   oc new-project kafka

   # Linux
   sed -i 's/namespace: .*/namespace: kafka/' install/cluster-operator/*.yaml

   # macOS
   sed -i '' 's/namespace: .*/namespace: kafka/' install/cluster-operator/*.yaml
   ```

   > The Cluster Operator watches the namespace set in the `STRIMZI_NAMESPACE` environment variable of its `Deployment` (defaults to the installation namespace in the manifests). To watch other namespaces, set `STRIMZI_NAMESPACE` to a comma-separated list or use `STRIMZI_NAMESPACE="*"` for all namespaces.

2. Install the Cluster Operator and wait for it to be available:

   ```bash
   oc create -f install/cluster-operator -n kafka

   oc wait --for=condition=Available deployment/strimzi-cluster-operator \
     -n kafka --timeout=180s
   ```

**Alternative: OperatorHub (OLM).** On clusters with the `redhat-operators` catalog, install the operator as the `amq-streams` package from the `amq-streams-3.2.x` channel (Streams for Apache Kafka 3.2) with `installPlanApproval: Manual`. The subscription in [`deploy/kafka/00-cluster-operator-olm.yaml`](../deploy/kafka/00-cluster-operator-olm.yaml) also **pins the exact tested CSV** via `startingCSV: amqstreams.v3.2.2-5` (see [Components and Tested Versions](../README.md#components-and-tested-versions) in the README) — Manual approval prevents OLM from auto-upgrading to a newer CSV in the channel, and `startingCSV` fixes the version a fresh install lands on. The operator watches the namespaces of its `OperatorGroup` — to watch additional namespaces later ([Step 7.1](07-http-bridge.md#71--create-the-namespace-and-extend-the-cluster-operator-watch)), add them to the OperatorGroup's `spec.targetNamespaces`; OLM regenerates the RBAC and rolls the operator automatically. A ready-to-apply Namespace + OperatorGroup + Subscription file is in [`deploy/kafka/00-cluster-operator-olm.yaml`](../deploy/kafka/00-cluster-operator-olm.yaml).

## 3.2 — Create the Kafka cluster

Kafka runs exclusively in **KRaft mode** (no ZooKeeper): you define the cluster with a `Kafka` resource plus `KafkaNodePool` resources. Nodes take the role of **broker**, **controller**, or both (dual-role). This example uses a single dual-role node pool with **persistent-claim storage** (`10Gi` per broker, `deleteClaim: false` so the PVCs survive a cluster uninstall; the `class` property is omitted, so PVCs come from the cluster's default StorageClass — set it if your cluster has no default or you want a specific one).

The ready-to-apply manifests are [`deploy/kafka/01-kafka-nodepool.yaml`](../deploy/kafka/01-kafka-nodepool.yaml) and [`deploy/kafka/02-kafka.yaml`](../deploy/kafka/02-kafka.yaml). **`02-kafka.yaml` contains `${OPENSHIFT_APPS_DOMAIN}`** (the brokers' `oauth.valid.issuer.uri`) — it must go through envsubst; plain `oc apply` leaves the literal `${...}` in the broker config and the rolled broker **crash-loops** with "Value of validIssuerUri not a valid URI" (the Kafka CR then goes NotReady, which empties `status.listeners` and breaks the console's Strimzi-kind connections):

```bash
export OPENSHIFT_APPS_DOMAIN=$(oc get ingress.config.openshift.io cluster -o jsonpath='{.spec.domain}')
envsubst '$OPENSHIFT_APPS_DOMAIN' < deploy/kafka/02-kafka.yaml | oc apply -f -
# (01-kafka-nodepool.yaml has no variables — plain oc apply is fine)

oc wait --for=condition=Ready kafka/kafka-cluster-1 -n kafka-cluster-1 --timeout=600s

oc get pods -n kafka-cluster-1
oc get services -n kafka-cluster-1
```

Field notes (full manifests in the deploy files):

- `KafkaNodePool` — `dual-role` nodes (`controller` + `broker` roles), `replicas: 3`, **persistent-claim storage** (`size: 10Gi`, `deleteClaim: false`; `class` omitted = the cluster's default StorageClass). ⚠️ Strimzi **cannot update** a node pool's storage — changing the type/size of an existing pool is not supported; a fresh deploy picks the new storage up, or delete and recreate the pool (and its PVCs) on an existing cluster. KRaft metadata shares the node's volume by default (the `kraftMetadata: shared` designation only matters for JBOD volumes).
- `spec.kafka.listeners` — `plain` (9092, no TLS/auth) + `tls` (9093, TLS with **SASL/OAUTHBEARER** — see the note below).
- `spec.kafka.config` — `inter.broker.listener.name: plain` (inter-broker communication stays on the unauthenticated listener), replication factors, `min.insync.replicas`, and **`auto.create.topics.enable: false`** — no auto-created topics (Kafka 4.x defaults to `true`): every topic must be created explicitly (`oc create topic`, a `KafkaTopic` resource, or the console), so a misspelled or un-created topic fails on produce instead of being silently created with the defaults.
- `spec.entityOperator` — **`topicOperator: {}`** reconciles `KafkaTopic` resources in the cluster's namespace (without it, `KafkaTopic` resources are inert — verified live). It sits under `spec`, a sibling of `kafka` — nesting it inside `spec.kafka` gets silently pruned and no operator deploys. ⚠️ The Topic Operator **cannot change a topic's replication factor**: if a topic already exists with a different RF than `spec.replicas`, reconciliation fails with "Replication factor change not supported" — the spec must match reality, or the topic must be deleted (via its `KafkaTopic`) and recreated by re-applying the manifest (how the demo topics moved from RF 1 to RF 3, [`deploy/kafka/03-topics.yaml`](../deploy/kafka/03-topics.yaml), verified live). A `userOperator: {}` can be added alongside to manage `KafkaUser` resources (not needed here — clients authenticate with Keycloak service accounts, Step 2.5).
- `spec.kafka.metricsConfig` — Strimzi Metrics Reporter on 9404 (Step 1.3).
- The deploy manifest creates **two** clusters (`kafka-cluster-1` and `kafka-cluster-2`), one per namespace — cluster-2 is the MirrorMaker 2 target ([Step 4](04-mirrormaker2.md#step-4-mirrormaker-2-between-the-two-clusters-optional)).

> In the `v1` API, `.spec.replicas` is required on the `Kafka` resource, and node resources (CPU/memory) are managed through `KafkaNodePool`. Streams for Apache Kafka 3.2 uses **static controller quorums** — controller node pools cannot be scaled after creation.

> **SASL/OAUTHBEARER note:** `authentication.type: custom` with `sasl: true` enables SASL/OAUTHBEARER over TLS (SASL_SSL) — OAUTHBEARER is the only mechanism on this listener. In Strimzi 0.51 the dedicated `authentication.type: oauth` listener type is deprecated, so OAuth uses `custom` auth with the Strimzi OAuth callback handlers; the operator prefixes `listenerConfig` keys with `listener.name.<listener>.`. Tokens are validated by the brokers against Keycloak's JWKS endpoint (the internal http service — for production use the TLS route and trust its CA). Client credentials are the `kafka` realm service-account clients created by [deploy/keycloak/04-kafka-realm-import.yaml](../deploy/keycloak/04-kafka-realm-import.yaml) — there are no `KafkaUser` credentials on this listener. `inter.broker.listener.name` must point at a non-OAuth listener (`plain` here).

You should see broker pods (`kafka-cluster-1-dual-role-0/1/2` — pods are named `<cluster-name>-<node-pool-name>-<id>` and the node pool here is `dual-role`) and the bootstrap Service `kafka-cluster-1-kafka-bootstrap`.

> **⚠️ Strimzi 0.51 note (listener NetworkPolicy):** the operator auto-generates a listener `NetworkPolicy` named `<cluster-name>-network-policy-kafka` for every `Kafka` cluster. When a listener has no `networkPolicyPeers`, that generated policy **admit-all namespaces on the listener port** — and because NetworkPolicy rules are additive, it silently defeats the Step 3.4 DMZ (we verified this on 4.22: kcat from an unrelated namespace got through even with a default-deny policy in place). Restrict the generated policy by setting `networkPolicyPeers` **on each listener** (it is a listener-level property, not a `spec.kafka.kafka` key). Set the same peers on every client-facing listener: same namespace + the `kafka-proxy` DMZ client. The ready-to-apply manifest [deploy/kafka/02-kafka.yaml](../deploy/kafka/02-kafka.yaml) already includes this for both listeners.

## 3.3 — Note the connection details for the proxy

This cluster's bootstrap address and broker IDs feed directly into the proxy's `KafkaService` ([Step 6.5](06-proxy.md#65--define-the-backend-cluster-kafkaservice)):

- `bootstrapServers`: `kafka-cluster-1-kafka-bootstrap.kafka-cluster-1.svc.cluster.local:9093` (tls listener — the proxy's upstream)
- Broker node IDs: `0`–`2` → `nodeIdRanges: { start: 0, end: 2 }`

Because the cluster uses the `tls` listener, the proxy can connect over TLS with the Strimzi-generated CA — `trustAnchorRef` on the `KafkaService` can auto-trust it (see [Step 6.5](06-proxy.md#65--define-the-backend-cluster-kafkaservice)). With OAUTHBEARER on that listener, the proxy relays the client's SASL/OAUTHBEARER exchange to the brokers (SASL passthrough) — the proxy cannot authenticate to the brokers itself, so every client of the virtual cluster must authenticate with OAUTHBEARER using a token from the `kafka` realm (see [Step 2.5](02-keycloak.md#25--client-authentication-at-the-proxy)).

> **Production notes:** use **dedicated controller and broker node pools** (at least 3 controllers), `persistent-claim` storage sized for your retention, the `tls` listener with authentication (`tls` client auth or OAuth), and consider Cruise Control for rebalancing. See [*Deploying and Managing Streams for Apache Kafka on OpenShift*](https://docs.redhat.com/en/documentation/red_hat_streams_for_apache_kafka/3.2/html/deploying_and_managing_streams_for_apache_kafka_on_openshift) for full configuration guidance.

## 3.4 — Restrict Kafka ingress to the proxy namespace (DMZ)

The Kafka cluster sits in the **secure backend**; the proxy is its only door. Because the proxy runs in a **DMZ**, lock down the cluster's network surface: the only namespace allowed to reach the Kafka listeners is `kafka-proxy`. Kubernetes `NetworkPolicy` rules are additive, so combine three policies on the Kafka pods (selected by the Strimzi label `strimzi.io/cluster: <cluster-name>`):

1. **Default deny** — block all ingress to Kafka pods.
2. **Same-namespace** — allow all traffic from within the cluster's namespace (controller↔broker on the control-plane port, broker↔broker replication, Cluster/Entity Operator, Cruise Control).
3. **Proxy-only clients** — allow ingress from the `kafka-proxy` namespace **only** on the client-facing listener ports (9092 plain, 9093 TLS).

The ready-to-apply manifest is [`deploy/kafka/03-kafka-netpol-dmz.yaml`](../deploy/kafka/03-kafka-netpol-dmz.yaml) — it covers **both** clusters (and includes the `kafka-mm2` allowance for the MirrorMaker 2 replication plane, [Step 4](04-mirrormaker2.md#step-4-mirrormaker-2-between-the-two-clusters-optional), on the plain listener only):

```bash
oc apply -f deploy/kafka/03-kafka-netpol-dmz.yaml
oc get networkpolicies -n kafka-cluster-1
```

Verify the DMZ behaves as intended (the `kafka-proxy` namespace is created by the
proxy operator install in [Step 6.1](06-proxy.md#61--install-the-proxy-operator) —
`oc new-project kafka-proxy` as a shortcut if you are running strictly in order):

```bash
# From another namespace (e.g., default): connection should TIME OUT
oc run kcat-blocked --rm -i --restart=Never --image=confluentinc/cp-kcat:latest -n default -- \
  -b kafka-cluster-1-kafka-bootstrap.kafka-cluster-1.svc.cluster.local:9092 -L

# From the kafka-proxy namespace: connection succeeds (this is what the proxy uses)
oc run kcat-allowed --rm -i --restart=Never --image=confluentinc/cp-kcat:latest -n kafka-proxy -- \
  -b kafka-cluster-1-kafka-bootstrap.kafka-cluster-1.svc.cluster.local:9092 -L
```

Notes:

- **Cluster Operator in another namespace?** The same-namespace rule covers the operator because this guide installs it into the cluster's namespace. If yours runs elsewhere, add a `namespaceSelector` entry for its namespace to policy 2.
- **Strimzi-generated listener policy:** since Streams 3.2 (Strimzi 0.51), the operator also creates `<cluster-name>-network-policy-kafka` — without per-listener `networkPolicyPeers` (see [Step 3.2](#32--create-the-kafka-cluster)) it permits **all namespaces** on the listener ports, which defeats this DMZ. After applying, list `oc get networkpolicies -n kafka-cluster-1` and confirm the generated policy's listener rules honour the peers you set.
- **Direct-broker checks**: with the DMZ in place, "consume directly from the broker" verification (e.g., checking ciphertext after enabling Record Encryption) must run from the cluster's or `kafka-proxy` namespace — from any other namespace it will be denied. That denial is the point.
- **Do not** add a `kubernetes.io/metadata.name: kafka` label requirement to policy 2 — podSelector rules already match by namespace; keep the same-namespace rule as shown.
- For defense in depth, also restrict the `kafka-proxy` namespace's **egress** to the Kafka brokers (plus the operator/API servers) so the proxy pod cannot be used as a pivot into the cluster.

## 3.5 — Quotas (per-principal, broker-enforced)

Kafka quotas are **broker-enforced rate limits** keyed per authenticated principal (`--entity-type users`) or per client id (`--entity-type clients`). Four knobs are available: `producer_byte_rate` (bytes/s of produce traffic), `consumer_byte_rate` (bytes/s of fetch traffic), `request_percentage` (broker CPU time share, %), and `controller_mutation_rate` (metadata mutations/s). Quotas govern **how fast** a principal may work — they complement, not replace, authorization: the proxy's ACL filter ([Step 6.6](06-proxy.md#66--optional-add-a-filter)) governs *what* a principal may do.

**Where the principal comes from in this stack:** the proxy relays the client's SASL/OAUTHBEARER exchange to the brokers (SASL passthrough — the proxy cannot authenticate upstream itself, [Step 2.5](02-keycloak.md#25--client-authentication-at-the-proxy)), so the broker session is authenticated as the **end client's Keycloak principal** — the token's `preferred_username`: `service-account-<clientId>` for the service-account clients (`service-account-camel-producer`, `service-account-kafka-bridge`, `service-account-demo-client`), or the realm username (`kafka-user-a`, `kafka-admin`, `demo`) for password-grant users. Per-user quotas therefore apply **to the end client through the proxy**; the proxy itself never appears as the quota principal. (A broker throttling a quota-exceeded session delays its responses, which propagates back through the proxy to the client.)

Set and verify quotas dynamically with `kafka-configs.sh` (standard Kafka tooling, shipped in the Strimzi image) — commands verified locally on a KRaft Kafka 4.1; the config is stored in the cluster's metadata (no broker restart, no client change):

```bash
# From a pod in the cluster's namespace — the plain listener (9092, no auth) is
# reachable same-namespace (the Step 3.4 DMZ denies it from everywhere else, which
# is fine: quota administration is a broker-side, operator-level task)
oc run kafka-quotas --rm -i --restart=Never --image=quay.io/strimzi/kafka:latest-kafka-4.2.2 -n kafka-cluster-1 -- \
  bin/kafka-configs.sh --bootstrap-server kafka-cluster-1-kafka-bootstrap:9092 \
  --alter --entity-type users --entity-name service-account-camel-producer \
  --add-config 'producer_byte_rate=1048576,consumer_byte_rate=2097152,request_percentage=50'

# Verify — quotas print as `user-principal '<name>'`
oc run kafka-quotas --rm -i --restart=Never --image=quay.io/strimzi/kafka:latest-kafka-4.2.2 -n kafka-cluster-1 -- \
  bin/kafka-configs.sh --bootstrap-server kafka-cluster-1-kafka-bootstrap:9092 \
  --describe --entity-type users
# Quota configs for user-principal 'service-account-camel-producer' are consumer_byte_rate=2097152.0, request_percentage=50.0, producer_byte_rate=1048576.0

# Remove one knob (re-running --alter with the same values is a no-op):
oc run kafka-quotas --rm -i --restart=Never --image=quay.io/strimzi/kafka:latest-kafka-4.2.2 -n kafka-cluster-1 -- \
  bin/kafka-configs.sh --bootstrap-server kafka-cluster-1-kafka-bootstrap:9092 \
  --alter --entity-type users --entity-name service-account-camel-producer \
  --delete-config 'request_percentage'
```

`--entity-type clients` keys on the `client.id` the **brokers** see — with the proxy relaying requests, prefer per-user quotas (per-client-id applies only if the client's `client.id` reaches the broker unchanged).

**Strimzi `KafkaUser` quotas — why not, for this stack:** a `KafkaUser` resource supports `spec.quota` (`producerByteRate`, `consumerByteRate`, `requestPercentage`, `controllerMutationRate`), but the User Operator keys the quota on the principal format of the KafkaUser's `spec.authentication.type` — `CN=<name>` for `tls`/`tls-external` (mTLS principals), `<name>` for `scram-sha-512`. There is **no OAuth authentication type for `KafkaUser`**, and this stack's listeners are OAUTHBEARER-only (Keycloak), so a `KafkaUser` named to match an OAuth principal would need `scram-sha-512` auth — creating an unusable SCRAM credential just to carry the quota. Setting the quota directly with `kafka-configs.sh` (above) keys it on the OAuth principal with no side-effect credentials. A `KafkaUser` with `spec.quota` remains the right tool when clients authenticate with certificates or SCRAM managed by Strimzi.
