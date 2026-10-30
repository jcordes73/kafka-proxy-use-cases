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

**Alternative: OperatorHub (OLM).** On clusters with the `redhat-operators` catalog, install the operator as the `amq-streams` package from the `amq-streams-3.2.x` channel (Streams for Apache Kafka 3.2) with `installPlanApproval: Manual`. The operator watches the namespaces of its `OperatorGroup` — to watch additional namespaces later ([Step 7.1](07-http-bridge.md#71--create-the-namespace-and-extend-the-cluster-operator-watch)), add them to the OperatorGroup's `spec.targetNamespaces`; OLM regenerates the RBAC and rolls the operator automatically. A ready-to-apply Namespace + OperatorGroup + Subscription file is in [`deploy/kafka/00-cluster-operator-olm.yaml`](../deploy/kafka/00-cluster-operator-olm.yaml).

## 3.2 — Create the Kafka cluster

Kafka runs exclusively in **KRaft mode** (no ZooKeeper): you define the cluster with a `Kafka` resource plus `KafkaNodePool` resources. Nodes take the role of **broker**, **controller**, or both (dual-role). This dev example uses a single dual-role node pool with ephemeral storage.

The ready-to-apply manifests are [`deploy/kafka/01-kafka-nodepool.yaml`](../deploy/kafka/01-kafka-nodepool.yaml) and [`deploy/kafka/02-kafka.yaml`](../deploy/kafka/02-kafka.yaml):

```bash
oc apply -f deploy/kafka/01-kafka-nodepool.yaml -f deploy/kafka/02-kafka.yaml

oc wait --for=condition=Ready kafka/kafka-cluster-1 -n kafka-cluster-1 --timeout=600s

oc get pods -n kafka-cluster-1
oc get services -n kafka-cluster-1
```

Field notes (full manifests in the deploy files):

- `KafkaNodePool` — `dual-role` nodes (`controller` + `broker` roles), `replicas: 3`, `ephemeral` storage for dev.
- `spec.kafka.listeners` — `plain` (9092, no TLS/auth) + `tls` (9093, TLS with **SASL/OAUTHBEARER** — see the note below).
- `spec.kafka.config` — `inter.broker.listener.name: plain` (inter-broker communication stays on the unauthenticated listener), replication factors, `min.insync.replicas`.
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
