# OpenShift Deployment Manifests

Ready-to-apply manifests for the full stack from [openshift-setup.md](../openshift-setup.md), organized per component. **Run all commands from the repository root** — every path below (and in the guides) is relative to it. Apply in **deployment order** — the user-workload monitoring stack first, then Keycloak, then the Kafka cluster, then MirrorMaker 2, then the Apicurio Registry, then the proxy (the HTTP Bridge and Console connect through it and follow), and the per-component monitoring *integration* — netpols, PodMonitors, console metrics source, alerts — after those components deploy. The Record Validation demo lives in [deploy/demos/](demos/02-topic-A (validation).yaml) ([Demo scenarios — Demo 2](../docs/11-demos.md#demo-2--record-validation-end-to-end)) and deploys with the optional demo scenarios:

```bash
# Deploy order 1 — user-workload monitoring stack (Step 1.1, cluster-admin, once)
oc apply -f deploy/observability/00-cluster-monitoring-config.yaml
oc -n openshift-user-workload-monitoring get pod   # prometheus-user-workload-0 Running

# Deploy order 2 — Keycloak (operator first, then dev PostgreSQL, then keycloak, realm import)
oc apply -f deploy/keycloak/

# Deploy order 3 — Kafka cluster, the secure backend (operator first, then cluster, then backend lockdown netpols)
oc apply -f deploy/kafka/00-cluster-operator-olm.yaml          # includes the kafka Namespace + OLM subscription (Manual approval — approve the InstallPlan)
oc apply -f deploy/kafka/01-kafka-nodepool.yaml -f deploy/kafka/02-kafka.yaml
oc wait --for=condition=Ready kafka/my-cluster -n kafka --timeout=600s
oc apply -f deploy/kafka/03-kafka-netpol-dmz.yaml

# Deploy order 4 — MirrorMaker 2 (Step 4, optional): replicates kafka-cluster-1 → kafka-cluster-2.
# The Cluster Operator must watch kafka-mm2 — extend the OperatorGroup before deploying.
# MM2 connects via the unauthenticated plain listener (9092), so no credentials or CA copies are needed.
oc patch operatorgroup kafka-operator-group -n kafka --type merge \
  -p '{"spec":{"targetNamespaces":["kafka","kafka-http-bridge","kafka-mm2"]}}'
oc apply -f deploy/mirrormaker2/01-mirrormaker2.yaml
oc wait --for=condition=Ready kafkamirrormaker2/kafka-cluster-1-to-2 -n kafka-mm2 --timeout=600s

# Deploy order 5 — Apicurio Registry (Steps 5.1–5.4): operator, dev PostgreSQL, registry, schema.
# Deploy order 6 extras — proxy ACLs (06-kafka-acl.yaml): apply WITHOUT envsubst
# (the ${configmap:...} interpolation ref must reach the operator).
# The registry CR (03-registry.yaml) embeds Routes + Keycloak auth — apply through envsubst.
# The OIDC truststore (keycloak-oidc-truststore) must be built with keytool from the
# Keycloak cert — an openssl-generated p12 loads with 0 entries in the JVM (Step 5.2).
# Realm import gotchas: don't declare service-account users (Keycloak auto-creates
# them — a duplicate entry fails the import); the import is skipped when the realm
# already exists (delete the realm + the import Job to re-run it).
# The Record Validation demo (Demo scenarios, Demo 2) is optional — its manifest moved to
# deploy/demos/02-topic-A (validation).yaml; it needs the proxy's virtual cluster, so it
# deploys with the demo scenarios (Step 11).
oc apply -f deploy/apicurio/01-subscription.yaml
oc -n kafka-registry patch installplan $(oc -n kafka-registry get installplan -o jsonpath='{.items[0].metadata.name}') --type merge -p '{"spec":{"approved":true}}'
oc apply -f deploy/apicurio/02-postgres.yaml
oc apply -f - <<EOF
apiVersion: v1
kind: Secret
metadata:
  name: registry-postgres-db
  namespace: kafka-registry
type: Opaque
stringData:
  username: registryuser
  password: registrypassword
EOF
oc apply -f deploy/apicurio/03-registry.yaml
oc -n kafka-registry wait --for=condition=Ready apicurioregistry3/apicurio-registry --timeout=600s
# 5.4 — register the schema (contentId feeds the Demo 2 filter): see docs/05-apicurio-registry.md
```

> **OAuth credentials for the data path:** the `tls` listener requires SASL/OAUTHBEARER — the service-account clients in the `kafka` realm ([`deploy/keycloak/04-kafka-realm-import.yaml`](keycloak/04-kafka-realm-import.yaml)) are the workloads' credentials. The realm import does **not** create Kubernetes Secrets — create one per workload in its own namespace before deploying the clients:
>
> ```bash
> oc create secret generic kafka-bridge-oauth --from-literal=client-secret=kafka-bridge-secret -n kafka-http-bridge
> oc create secret generic camel-producer-oauth --from-literal=client-secret=camel-producer-secret -n kafka-producer
> oc create secret generic camel-consumer-oauth --from-literal=client-secret=camel-consumer-secret -n kafka-consumer
> oc create secret generic kafka-console-kafka-oauth --from-literal=client-secret=kafka-console-kafka-secret -n kafka-console
> # demo-client-oauth in kafka-proxy — see docs/06-proxy.md Step 6.10 (smoke tests)
> ```

## Namespaces ship with each component

There is no central namespaces file — each component's first manifest contains
its own `Namespace`, so applying a component's directory is self-contained:

| Component manifest | Namespace created |
|---|---|
| `deploy/kafka/00-cluster-operator-olm.yaml` | `kafka` |
| `deploy/keycloak/01-kafka-auth-operator.yaml` | `kafka-auth` |
| `deploy/proxy/01-proxy-operator-install.yaml` | `kafka-proxy` |
| `deploy/bridge/01-kafka-bridge.yaml` | `kafka-http-bridge` |
| `deploy/bridge/02-bridge-netpol-dmz.yaml` | `api-gateway` (stand-in for the DMZ verification) |
| `deploy/mirrormaker2/01-mirrormaker2.yaml` | `kafka-mm2` |
| `deploy/console/01-console-operator.yaml` | `kafka-console` |
| `deploy/camel/producer/00-buildconfig.yaml` | `kafka-producer` |
| `deploy/camel/consumer/00-buildconfig.yaml` | `kafka-consumer` |
| `deploy/custom-filter/01-buildconfig.yaml` | `kafka-filter-dev` |
| `deploy/apicurio/01-subscription.yaml` | `kafka-registry` |
| `demos/06-authorization-filters.yaml` | `kafka-proxy` (replaces the `kafka-cluster-1` VirtualKafkaCluster wiring) |
| `demos/07-entity-isolation.yaml` | `kafka-proxy` (replaces the `kafka-cluster-1` VirtualKafkaCluster wiring) |
| `demos/08-token-validation-filters.yaml` | `kafka-proxy` (replaces the `kafka-cluster-1` VirtualKafkaCluster wiring) |
| `demos/09-multi-tenant-virtual-clusters.yaml` | `kafka-proxy` |
| `demos/10-mm2-failover.yaml` | `kafka-mm2` (replaces the running KafkaMirrorMaker2) |
| `demos/11-record-encryption.yaml` | `kafka-vault` + `kafka-proxy` |

| Directory | Component | Guide step (= deploy order) |
|---|---|---|
| `deploy/observability/00-cluster-monitoring-config.yaml` | User-workload monitoring stack enablement (`enableUserWorkload`, cluster-admin, once) | [1.1](../docs/01-user-workload-monitoring.md#11--enable-monitoring-for-user-defined-projects-deploy-this-first-cluster-admin-once) |
| `deploy/keycloak/` | Red Hat build of Keycloak (kafka-auth namespace): operator subscription, dev PostgreSQL, `keycloak`, in-cluster http Service (`keycloak-http`), `kafka` realm import (console OIDC client + data-path service-account clients) | [2](../docs/02-keycloak.md) |
| `deploy/kafka/` | Streams for Apache Kafka operator (OLM) + Kafka cluster (OAUTHBEARER-only tls listener) | [3](../docs/03-kafka-cluster.md) |
| `deploy/mirrormaker2/` | MirrorMaker 2 replication between the two backend clusters (secure backend; optional; replicates as soon as both clusters are up) | [4](../docs/04-mirrormaker2.md) |
| `deploy/apicurio/01–04` | Red Hat build of Apicurio Registry 3 (operator subscription, dev PostgreSQL, `ApicurioRegistry3`, DMZ netpols) + schema registration | [5.1–5.4](../docs/05-apicurio-registry.md) |
| `deploy/proxy/` | KafkaProxy, ingress, KafkaService, VirtualKafkaCluster (kafka-proxy namespace) | [6](../docs/06-proxy.md) |
| `deploy/bridge/` | Kafka HTTP Bridge (DMZ client) + DMZ NetworkPolicies (kafka-http-bridge); connects through the proxy | [7](../docs/07-http-bridge.md) |
| `deploy/console/` | Streams for Apache Kafka Console (kafka-console): operator OLM subscription (`amq-streams-console`), `Console` CR with OIDC, metrics Services + ServiceMonitors | [8](../docs/08-console.md) |
| `deploy/camel/producer`, `deploy/camel/consumer` | Camel Quarkus client projects (source + image build + Deployment) | [9](../docs/09-camel-clients.md) |
| `deploy/observability/01–05` | Per-component monitoring integration: metrics netpols, broker/proxy/bridge PodMonitors, alerts | [1.2–1.9](../docs/01-user-workload-monitoring.md) |
| `deploy/custom-filter/` | Custom filter plugin: Dockerfile + image build + `KafkaProtocolFilter` (requires the local Maven filter project from [Step 10.1](../docs/10-custom-filter.md#101--generate-and-test-a-filter-project)) | [10](../docs/10-custom-filter.md) |
| `deploy/demos/` | Demo manifests (Step 11, optional): **Record Validation demo** (Demo 2 — topic + filter wiring, `02-topic-A (validation).yaml`), proxy filters (authorization, entity isolation, token validation), multi-tenant virtual clusters, MM2 failover variant, Record Encryption with a dev Vault | [11](../docs/11-demos.md) |

## Templating: `OPENSHIFT_APPS_DOMAIN`

Manifests that embed cluster-specific hostnames (Route hostnames, the Keycloak
issuer/JWKS URLs, the console OIDC server) use the `${OPENSHIFT_APPS_DOMAIN}`
variable — the cluster's default Ingress Controller domain (the suffix of every
Route hostname). Source `env.sh` to set it from the live cluster, then apply
the affected files through `envsubst`:

```bash
source deploy/env.sh

envsubst < deploy/keycloak/04-kafka-realm-import.yaml | oc apply -f -   # console redirect URI
envsubst < deploy/console/02-kafka-console.yaml | oc apply -f -  # console hostname + OIDC
```

Files without `${OPENSHIFT_APPS_DOMAIN}` can be applied directly with
`oc apply -f <file>`. (`envsubst` leaves other `${...}` sequences — such as
Strimzi config-provider references — untouched.)

`deploy/demos/02-topic-A (validation).yaml` additionally uses
`${APICURIO_CONTENT_ID}` — the schema's `contentId` from the registry, obtained
in [Step 5.4](../docs/05-apicurio-registry.md#54--register-a-schema):

```bash
export APICURIO_CONTENT_ID=1
envsubst < deploy/demos/02-topic-A (validation).yaml | oc apply -f -
```

## What stays imperative

Everything declarative is in this directory — each component's first manifest
creates its own namespace (see the table above), and every resource the stack
needs (operators,
Kafka, Keycloak, proxy, bridge, console, Camel BuildConfigs, monitoring) is a
manifest. The guide's remaining `oc` commands are deliberately imperative:

- **Secrets and TLS material** — credentials and certificates never enter git. The guide
  creates them inline as `oc apply -f -` heredocs (`stringData`, or file content indented
  via `sed`), so re-running a step is idempotent: identical values are a no-op (the CLI
  prints `configured`, but the object is not modified) instead of failing with
  `AlreadyExists`. Re-applying with changed values updates the Secret in place; workloads
  pick up the new value on their next pod (re)start.
- **Binary image builds** (`oc start-build --from-dir`) — the payload is the local
  Maven build output; nothing to declare.
- **Verification** (`oc run` smoke-test pods, `oc wait`, `oc logs`, `oc get`) —
  read-only or ephemeral checks, not cluster state.

Placeholders in manifest files:

- `<KEYCLOAK_HOST>` — the externally reachable hostname of the `keycloak` Keycloak route (filled in after [Step 2.3](../docs/02-keycloak.md#23--tls-certificate-and-keycloak-deployment) — see [openshift-setup.md, Step 2](../docs/02-keycloak.md#step-2-install-red-hat-build-of-keycloak-kafka-auth-namespace)). Derived as `keycloak-ingress-kafka-auth.${OPENSHIFT_APPS_DOMAIN}`; used by the console OIDC configuration.

Secrets (Keycloak TLS material, the console OIDC client secret) are created with `oc create` commands from the guide — they are deliberately NOT in git.
