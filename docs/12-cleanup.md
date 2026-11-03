# Step 12: Clean up (teardown)

> [↑ Setup overview](../openshift-setup.md) · next: — (end of the guide)

Tearing the platform down in the **reverse of the deployment order** (see the [Guide map](../openshift-setup.md#guide-map)): the clients that depend on other components go first, the infrastructure they depended on goes last. All commands run from the **repository root**.

Two ordering rules matter more than the exact sequence:

1. **Delete the custom resources before their operators.** Strimzi, Apicurio, proxy, and console CRs carry finalizers that the operator removes — deleting an operator (or its namespace) first leaves the CR stuck in `Terminating`.
2. **Delete the CRs before their namespaces.** Namespace deletion wipes the StatefulSets and PVCs underneath, but a namespace deleted before its CR can leave finalizer-stuck resources behind.

## 12.1 — Delete the demo and client workloads

```bash
# Camel clients (Step 9) — namespace deletion removes the integrations,
# builds, and the oauth Secrets:
oc delete project kafka-producer kafka-consumer

# Console (Step 8):
oc delete consoles.console.streamshub.github.com/kafka-console -n kafka-console
oc delete project kafka-console

# HTTP Bridge (Step 7):
oc delete kafkabridge/my-bridge -n kafka-http-bridge
oc delete project kafka-http-bridge api-gateway   # api-gateway is the DMZ verification stand-in (deploy/bridge/02-bridge-netpol-dmz.yaml)

# Demo manifests (Step 10) — filters/virtual-cluster wiring from the demo
# directories, if applied (they replace or add VirtualKafkaCluster resources):
oc delete kafkaprotocolfilter record-validation -n kafka-proxy --ignore-not-found
```

## 12.2 — Delete the proxy (Step 6)

The `KafkaProxy` CR owns the runtime `deployment/simple`; the virtual clusters and their filter references go first so the proxy reconciles down cleanly:

```bash
oc delete virtualkafkacluster --all -n kafka-proxy
oc delete kafkaproxyingress --all -n kafka-proxy
oc delete kafkaservice --all -n kafka-proxy
oc delete kafkaproxy --all -n kafka-proxy
oc delete project kafka-proxy
```

The cluster CA Secrets **copied into** `kafka-proxy` (Step 6, `trustStrimziCaCertificate`) go with the namespace. The proxy operator lives in `openshift-operators` (cluster-scoped) — removed in [12.6](#126--remove-the-operators-and-cluster-scoped-leftovers).

## 12.3 — Delete the Apicurio Registry (Step 5)

```bash
oc delete apicurioregistry3/apicurio-registry -n kafka-registry
# The PostgreSQL StatefulSet is not operator-managed — delete it explicitly:
oc delete statefulset/registry-postgresql-db service/registry-postgresql-db-service service/registry-postgres-db -n kafka-registry
oc delete project kafka-registry   # deletes the registry-postgresql-data PVC (5Gi EBS volume — data gone)
```

## 12.4 — Delete MirrorMaker 2 (Step 4)

```bash
oc delete kafkamirrormaker2/kafka-cluster-1-to-2 -n kafka-mm2
oc delete project kafka-mm2
```

MM2's remote-cluster topics (`heartbeats`, `cluster1.checkpoints.internal`, …) on `kafka-cluster-2` are removed with the cluster in [12.5](#125--delete-the-kafka-clusters-and-their-pvcs).

## 12.5 — Delete the Kafka clusters and their PVCs

```bash
# CRs first (the operator removes the finalizers), then the node pools:
oc delete kafka/kafka-cluster-1 -n kafka-cluster-1
oc delete kafka/kafka-cluster-2 -n kafka-cluster-2
oc delete kafkanodepool --all -n kafka-cluster-1
oc delete kafkanodepool --all -n kafka-cluster-2
```

> **The broker PVCs survive on purpose.** The node pool storage is `persistent-claim` with **`deleteClaim: false`** ([Step 3.2](03-kafka-cluster.md#32--create-the-kafka-cluster)) — the operator leaves `data-<cluster>-dual-role-*` PVCs behind so an uninstall is reversible. The cluster data lives on these PVCs (KRaft mode: metadata **and** records). To remove them too:
>
> ```bash
> oc -n kafka-cluster-1 delete pvc --all
> oc -n kafka-cluster-2 delete pvc --all
> ```

```bash
oc delete project kafka-cluster-1 kafka-cluster-2 kafka
```

The demo topics (`topic-A/B/C`, [Step 3.2](03-kafka-cluster.md#32--create-the-kafka-cluster) / `deploy/kafka/03-topics.yaml`) were created directly with `kafka-topics.sh` — they live on the cluster data and go with the PVCs, not as separate resources.

## 12.6 — Remove the operators and cluster-scoped leftovers

```bash
# AMQ Streams (kafka namespace — owns the Kafka/NodePool/MM2 CRDs):
oc -n kafka delete subscription amq-streams
# Keycloak operator (kafka-auth) — delete the realm import + Keycloak CRs first
# (the operator must remove their finalizers):
oc -n kafka-auth delete keycloakrealmimport/kafka-realm-import
oc -n kafka-auth delete keycloak/keycloak
oc -n kafka-auth delete subscription rhbk-operator
# Apicurio Registry operator (kafka-registry):
oc -n kafka-registry delete subscription apicurio-registry-3
# Proxy and Console operators installed cluster-wide into openshift-operators:
oc -n openshift-operators delete subscription $(oc -n openshift-operators get subscription -o name | grep -E "amq-streams-proxy|streamshub-console")
```

Then the namespaces (this removes the remaining operator pods, CSVs, secrets, and the Keycloak DB PVC):

```bash
oc delete project kafka kafka-auth
```

## 12.7 — Remove the monitoring stack (Step 1)

The [cluster monitoring config](01-user-workload-monitoring.md#11--enable-monitoring-for-user-defined-projects-deploy-this-first-cluster-admin-once) is **cluster-scoped** (`cluster-monitoring-config` in `openshift-monitoring`) and is not removed by any namespace deletion:

```bash
oc -n openshift-monitoring delete configmap cluster-monitoring-config
```

The PodMonitors, alerts, netpols, and Perses resources live in the component namespaces and are gone with [12.1–12.6](#121--delete-the-demo-and-client-workloads).

## 12.8 — Verify nothing is left

```bash
oc get projects | grep -E "kafka|api-gateway"          # no rows
oc get csv --all-namespaces | grep -iE "amq|streamshub|apicurio"   # no rows
oc get crd | grep -iE "strimzi|apicurio|kroxylicious|console.streamshub|keycloak"   # CRDs remain (harmless — see below)
```

> **CRDs are cluster-scoped and persist.** OLM removes the operators' CSVs and pods, but the CRDs installed by the operators (`kafkas.kafka.strimzi.io`, `apicurioregistries3.registry.apicur.io`, `kafkaproxyingresses.kroxylicious.io`, …) stay on the cluster. They are empty schemas at this point and do not block a re-install — leave them unless you need the cluster fully pristine, in which case delete them by name (`oc delete crd <name>`).

## What a teardown does NOT touch

- **Cluster state from earlier steps that lives outside these resources** — e.g. the [Keycloak `master` realm admin user](02-keycloak.md#23--tls-certificate-and-keycloak-deployment) (re-created by the operator on a fresh install) and any OpenShift-level configuration you made outside this guide.
- **The sandbox cluster itself** — an OpenShift sandbox may expire independently; the entire guide is reproducible from scratch via the [deployment order](../deploy/README.md).
