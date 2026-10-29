# OpenShift Deployment Manifests

Ready-to-apply manifests for the full stack from [openshift-setup.md](../openshift-setup.md), organized per component. **Run all commands from the repository root** — every path below (and in the guides) is relative to it. Apply in guide order:

```bash
# Step 0 — Kafka cluster (operator first, then cluster, then DMZ netpols)
oc apply -f deploy/kafka/00-cluster-operator-olm.yaml          # includes the kafka Namespace + OLM subscription (Manual approval — approve the InstallPlan)
oc apply -f deploy/kafka/01-kafka-nodepool.yaml -f deploy/kafka/02-kafka.yaml
oc wait --for=condition=Ready kafka/my-cluster -n kafka --timeout=600s
oc apply -f deploy/kafka/03-kafka-netpol-dmz.yaml
```

> **OAuth credentials for the data path:** the `tls` listener requires SASL/OAUTHBEARER — the service-account clients in the `kafka` realm ([`deploy/keycloak/04-kafka-realm-import.yaml`](keycloak/04-kafka-realm-import.yaml)) are the workloads' credentials. The realm import does **not** create Kubernetes Secrets — create one per workload in its own namespace before deploying the clients:
>
> ```bash
> oc create secret generic kafka-bridge-oauth --from-literal=client-secret=kafka-bridge-secret -n kafka-http-bridge
> oc create secret generic camel-producer-oauth --from-literal=client-secret=camel-producer-secret -n kafka-producer
> oc create secret generic camel-consumer-oauth --from-literal=client-secret=camel-consumer-secret -n kafka-consumer
> oc create secret generic kafka-console-kafka-oauth --from-literal=client-secret=kafka-console-kafka-secret -n kafka-console
> # demo-client-oauth in kafka-proxy — see docs/03-proxy.md Step 11 (smoke tests)
> ```
>
> **MirrorMaker 2 ([Step 18](../docs/10-mirrormaker2.md), optional):** the Cluster Operator must watch `kafka-mm2` — extend the OperatorGroup before deploying. MM2 connects via the unauthenticated `plain` listener (9092), so no credentials or CA copies are needed:
>
> ```bash
> oc patch operatorgroup kafka-operator-group -n kafka --type merge \
>   -p '{"spec":{"targetNamespaces":["kafka","kafka-http-bridge","kafka-mm2"]}}'
> oc apply -f deploy/mirrormaker2/01-mirrormaker2.yaml
> oc wait --for=condition=Ready kafkamirrormaker2/kafka-cluster-1-to-2 -n kafka-mm2 --timeout=600s
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

| Directory | Component | Guide step |
|---|---|---|
| `deploy/kafka/` | Streams for Apache Kafka operator (OLM) + Kafka cluster (OAUTHBEARER-only tls listener) | [0](../docs/01-kafka-cluster.md) |
| `deploy/keycloak/` | Red Hat build of Keycloak (kafka-auth namespace): operator subscription, dev PostgreSQL, `keycloak`, in-cluster http Service (`keycloak-http`), `kafka` realm import (console OIDC client + data-path service-account clients) | [1](../docs/02-keycloak.md) |
| `deploy/proxy/` | KafkaProxy, ingress, KafkaService, VirtualKafkaCluster (kafka-proxy namespace) | [2–11](../docs/03-proxy.md) |
| `deploy/bridge/` | Kafka HTTP Bridge + DMZ NetworkPolicies (kafka-http-bridge) | [12](../docs/04-http-bridge.md) |
| `deploy/console/` | Streams for Apache Kafka Console (kafka-console): operator OLM subscription (`amq-streams-console`), `Console` CR with OIDC, metrics Services + ServiceMonitors | [13](../docs/05-console.md) |
| `deploy/camel/producer`, `deploy/camel/consumer` | Camel Quarkus client projects (source + image build + Deployment) | [14](../docs/06-camel-clients.md) |
| `deploy/observability/` | OpenShift user-workload monitoring integration (pod/service monitors, metrics netpols, alerts) | [15](../docs/07-observability.md) |
| `deploy/custom-filter/` | Custom filter plugin: Dockerfile + image build + `KafkaProtocolFilter` (requires the local Maven filter project from [Step 16.1](../docs/08-custom-filter.md#161--generate-and-test-a-filter-project)) | [16](../docs/08-custom-filter.md) |
| `deploy/apicurio/` | Red Hat build of Apicurio Registry 3 (operator subscription, dev PostgreSQL, `ApicurioRegistry3`, DMZ netpols) + Record Validation demo (topic + filter) | [17](../docs/09-apicurio-registry.md) |
| `deploy/demos/` | Demo manifests: proxy filters (authorization, entity isolation, token validation), multi-tenant virtual clusters, MM2 failover variant, Record Encryption with a dev Vault | [19](../docs/11-demos.md) |
| `deploy/mirrormaker2/` | MirrorMaker 2 replication between the two clusters (optional; requires the DMZ allowance from Step 0.4) | [18](../docs/10-mirrormaker2.md) |

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

`deploy/apicurio/05-record-validation-demo.yaml` additionally uses
`${APICURIO_CONTENT_ID}` — the schema's `contentId` from the registry, obtained
in [Step 17.4](../docs/09-apicurio-registry.md#174--register-a-schema):

```bash
export APICURIO_CONTENT_ID=1
envsubst < deploy/apicurio/05-record-validation-demo.yaml | oc apply -f -
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

- `<KEYCLOAK_HOST>` — the externally reachable hostname of the `keycloak` Keycloak route (filled in after [Step 1.3](../docs/02-keycloak.md#13--tls-certificate-and-keycloak-deployment) — see [openshift-setup.md, Step 1](../docs/02-keycloak.md#step-1-install-red-hat-build-of-keycloak-kafka-auth-namespace)). Derived as `keycloak-ingress-kafka-auth.${OPENSHIFT_APPS_DOMAIN}`; used by the console OIDC configuration.

Secrets (Keycloak TLS material, the console OIDC client secret) are created with `oc create` commands from the guide — they are deliberately NOT in git.
