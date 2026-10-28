# OpenShift Deployment Manifests

Ready-to-apply manifests for the full stack from [openshift-setup.md](../openshift-setup.md), organized per component. Apply in guide order:

```bash
# Step 0 — Kafka cluster (operator first, then cluster, then DMZ netpols)
oc apply -f kafka/00-cluster-operator-olm.yaml          # includes the kafka Namespace + OLM subscription (Manual approval — approve the InstallPlan)
oc apply -f kafka/01-kafka-nodepool.yaml -f kafka/02-kafka.yaml
oc wait --for=condition=Ready kafka/my-cluster -n kafka --timeout=600s
oc apply -f kafka/03-kafka-netpol-dmz.yaml
```

## Namespaces ship with each component

There is no central namespaces file — each component's first manifest contains
its own `Namespace`, so applying a component's directory is self-contained:

| Component manifest | Namespace created |
|---|---|
| `kafka/00-cluster-operator-olm.yaml` | `kafka` |
| `keycloak/01-kafka-auth-operator.yaml` | `kafka-auth` |
| `proxy/01-proxy-operator-install.yaml` | `kafka-proxy` |
| `bridge/01-kafka-bridge.yaml` | `kafka-http-bridge` |
| `bridge/02-bridge-netpol-dmz.yaml` | `api-gateway` (stand-in for the DMZ verification) |
| `console/01-console-operator.yaml` | `kafka-console` |
| `camel/producer/00-buildconfig.yaml` | `kafka-producer` |
| `camel/consumer/00-buildconfig.yaml` | `kafka-consumer` |
| `custom-filter/01-buildconfig.yaml` | `kafka-filter-dev` |

| Directory | Component | Guide step |
|---|---|---|
| `kafka/` | Streams for Apache Kafka operator (OLM) + Kafka cluster | 0 |
| `keycloak/` | Red Hat build of Keycloak (kafka-auth namespace): operator subscription, dev PostgreSQL, `keycloak`, `kafka` realm import | 1 |
| `proxy/` | KafkaProxy, ingress, KafkaService, VirtualKafkaCluster (kafka-proxy namespace) | 2–11 |
| `bridge/` | Kafka HTTP Bridge + DMZ NetworkPolicies (kafka-http-bridge) | 12 |
| `console/` | Streams for Apache Kafka Console (kafka-console) | 13 |
| `camel/producer`, `camel/consumer` | Camel Quarkus client projects (source + image build + Deployment) | 14 |
| `observability/` | OpenShift user-workload monitoring integration (pod/service monitors, metrics netpols, alerts) | 15 |
| `custom-filter/` | Custom filter plugin: Dockerfile + image build + `KafkaProtocolFilter` (requires the local Maven filter project from Step 16.1) | 16 |

## Templating: `OPENSHIFT_APPS_DOMAIN`

Manifests that embed cluster-specific hostnames (Route hostnames, the Keycloak
issuer/JWKS URLs, the console OIDC server) use the `${OPENSHIFT_APPS_DOMAIN}`
variable — the cluster's default Ingress Controller domain (the suffix of every
Route hostname). Source `env.sh` to set it from the live cluster, then apply
the affected files through `envsubst`:

```bash
source deploy/env.sh

envsubst < keycloak/04-kafka-realm-import.yaml | oc apply -f -   # console redirect URI
envsubst < console/02-kafka-console.yaml | oc apply -f -  # console hostname + OIDC
```

Files without `${OPENSHIFT_APPS_DOMAIN}` can be applied directly with
`oc apply -f <file>`. (`envsubst` leaves other `${...}` sequences — such as
Strimzi config-provider references — untouched.)

## What stays imperative

Everything declarative is in this directory — each component's first manifest
creates its own namespace (see the table above), and every resource the stack
needs (operators,
Kafka, Keycloak, proxy, bridge, console, Camel BuildConfigs, monitoring) is a
manifest. The guide's remaining `oc` commands are deliberately imperative:

- **Secrets and TLS material** (`oc create secret …`, `oc create configmap proxy-ca`) —
  credentials and certificates never enter git; create them with the guide's commands.
- **Binary image builds** (`oc start-build --from-dir`) — the payload is the local
  Maven build output; nothing to declare.
- **Verification** (`oc run` smoke-test pods, `oc wait`, `oc logs`, `oc get`) —
  read-only or ephemeral checks, not cluster state.

Placeholders in manifest files:

- `<KEYCLOAK_HOST>` — the externally reachable hostname of the `keycloak` Keycloak route (filled in after Step 1.3 — see [openshift-setup.md, Step 1](../openshift-setup.md#step-1-install-red-hat-build-of-keycloak-kafka-auth-namespace)). Derived as `keycloak-ingress-kafka-auth.${OPENSHIFT_APPS_DOMAIN}`; used by the console OIDC configuration.

Secrets (Keycloak TLS material, the console OIDC client secret) are created with `oc create` commands from the guide — they are deliberately NOT in git.
