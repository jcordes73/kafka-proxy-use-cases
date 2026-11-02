# OpenShift Deployment Manifests

Ready-to-apply manifests for the full stack from [openshift-setup.md](../openshift-setup.md), organized per component. **Run all commands from the repository root** — every path below (and in the guides) is relative to it. Apply in **deployment order** — the user-workload monitoring stack first, then Keycloak, then the Kafka cluster, then MirrorMaker 2, then the Apicurio Registry, then the proxy (the HTTP Bridge and Console connect through it and follow), and the per-component monitoring *integration* — netpols, PodMonitors, console metrics source, alerts — after those components deploy. The Record Validation demo lives in [`deploy/demos/02-record-validation-demo.yaml`](demos/02-record-validation-demo.yaml) ([Demo scenarios — Demo 2](../docs/demos/02-record-validation.md)) and deploys with the proxy (its filter is referenced by the virtual clusters):

```bash
# Deploy order 1 — user-workload monitoring stack (Step 1.1, cluster-admin, once)
oc apply -f deploy/observability/00-cluster-monitoring-config.yaml
# Gate on creation first: a condition/jsonpath wait on a not-yet-created resource
# blocks until timeout instead of failing fast (verified live on a fresh cluster).
oc -n openshift-user-workload-monitoring wait --for=create pod/prometheus-user-workload-0 --timeout=300s
oc -n openshift-user-workload-monitoring wait --for=condition=Ready pod/prometheus-user-workload-0 --timeout=600s

# Deploy order 2 — Keycloak (operator first, then dev PostgreSQL, then keycloak, realm import).
# The kafka-auth namespace does not exist yet on a fresh cluster — create it from its manifest
# BEFORE the Secrets, or the Secret creates fail with `namespaces "kafka-auth" not found`
# (verified live; applying the deploy/keycloak/ directory later is idempotent):
oc apply -f deploy/keycloak/00-namespace.yaml
# The Keycloak pod mounts two Secrets BEFORE it can start — create them first (Steps 2.2/2.3):
oc apply -f - <<'EOF'
apiVersion: v1
kind: Secret
metadata:
  name: keycloak-db-secret
  namespace: kafka-auth
type: Opaque
stringData:
  username: testuser
  password: testpassword
EOF
source deploy/env.sh   # the TLS SAN must carry the operator-assigned Route hostname (Step 2.3)
openssl req -new -newkey rsa:2048 -nodes \
  -subj '/CN=kafka-auth/O=Kafka Auth./C=US' \
  -addext "subjectAltName=DNS:keycloak-ingress-kafka-auth.${OPENSHIFT_APPS_DOMAIN}" \
  -keyout key.pem -x509 -days 365 -out certificate.pem
oc -n kafka-auth create secret tls keycloak-tls-secret --cert=certificate.pem --key=key.pem
rm -f key.pem certificate.pem
oc apply -f deploy/keycloak/
# The operator's InstallPlan is Manual — gate on creation, approve, then wait for the CSV.
# The CSV only exists AFTER approval, so a phase wait started earlier blocks until timeout:
oc -n kafka-auth wait --for=create installplan --all --timeout=300s
oc -n kafka-auth patch installplan $(oc -n kafka-auth get installplan -o jsonpath='{.items[0].metadata.name}') --type merge -p '{"spec":{"approved":true}}'
oc -n kafka-auth wait --for=create csv --all --timeout=300s
oc -n kafka-auth wait --for=jsonpath='{.status.phase}'=Succeeded csv --all --timeout=600s
# The realm import manifest embeds ${OPENSHIFT_APPS_DOMAIN} (console redirect URI) — the raw
# apply in deploy/keycloak/ fails with "Invalid client kafka-console: A redirect URI is not a
# valid URI". Delete the CR + its failed Job and re-apply through envsubst:
oc -n kafka-auth delete keycloakrealmimport kafka-realm-import --ignore-not-found
oc -n kafka-auth delete job kafka-realm-import --ignore-not-found
envsubst < deploy/keycloak/04-kafka-realm-import.yaml | oc apply -f -
oc -n kafka-auth wait --for=create job --all --timeout=300s
oc -n kafka-auth wait --for=condition=Complete job --all --timeout=300s
oc -n kafka-auth wait --for=condition=Ready keycloak/keycloak --timeout=600s

# Deploy order 3 — Kafka cluster, the secure backend (operator first, then cluster, then backend lockdown netpols).
# The operator's OperatorGroup targets namespaces its components create later — on a fresh
# cluster they don't exist yet and the operator crash-loops ("pods is forbidden" listing pods
# in kafka-http-bridge, then kafka-cluster-1/2) until they do. Create them from their manifests
# (verified live; applying the component directories later is idempotent):
oc apply -f deploy/kafka/00-cluster-namespaces.yaml -f deploy/mirrormaker2/00-namespace.yaml
oc apply -f deploy/kafka/00-cluster-operator-olm.yaml          # OLM subscription (Manual approval — approve the InstallPlan)
# Gate on creation, approve the Manual InstallPlan, then wait for the CSV (which only
# exists after approval — a phase wait started earlier blocks until timeout):
oc -n kafka wait --for=create installplan --all --timeout=300s
oc -n kafka patch installplan $(oc -n kafka get installplan -o jsonpath='{.items[0].metadata.name}') --type merge -p '{"spec":{"approved":true}}'
oc -n kafka wait --for=create csv --all --timeout=300s
oc -n kafka wait --for=jsonpath='{.status.phase}'=Succeeded csv --all --timeout=600s
# The manifest's OperatorGroup omits kafka-mm2 — extend it before deploying MM2 (deploy order 4):
oc -n kafka patch operatorgroup kafka-operator-group --type merge \
  -p '{"spec":{"targetNamespaces":["kafka","kafka-cluster-1","kafka-cluster-2","kafka-http-bridge","kafka-mm2"]}}'
# 02-kafka.yaml embeds ${OPENSHIFT_APPS_DOMAIN} — apply through envsubst:
source deploy/env.sh
oc apply -f deploy/kafka/01-kafka-nodepool.yaml
envsubst < deploy/kafka/02-kafka.yaml | oc apply -f -
oc -n kafka-cluster-1 wait --for=condition=Ready kafka/kafka-cluster-1 --timeout=900s
oc -n kafka-cluster-2 wait --for=condition=Ready kafka/kafka-cluster-2 --timeout=900s
oc apply -f deploy/kafka/03-kafka-netpol-dmz.yaml
oc apply -f deploy/kafka/03-topics.yaml    # demo topics (topic-A/B/C) — clients hit UNKNOWN_TOPIC_OR_PARTITION without them

# Deploy order 4 — MirrorMaker 2 (Step 4, optional): replicates kafka-cluster-1 → kafka-cluster-2.
# The OperatorGroup was extended to kafka-mm2 above. MM2 connects via the unauthenticated
# plain listener (9092), so no credentials or CA copies are needed.
oc apply -f deploy/mirrormaker2/01-mirrormaker2.yaml
oc -n kafka-mm2 wait --for=condition=Ready kafkamirrormaker2/kafka-cluster-1-to-2 --timeout=900s

# Deploy order 5 — Apicurio Registry (Steps 5.1–5.4): operator, dev PostgreSQL, registry, schema.
# The registry CR (03-registry.yaml) embeds Routes + Keycloak auth — apply through envsubst.
# The OIDC truststore (keycloak-oidc-truststore) must be built with keytool from the
# Keycloak cert — an openssl-generated p12 loads with 0 entries in the JVM (Step 5.2).
# Realm import gotchas: don't declare service-account users (Keycloak auto-creates
# them — a duplicate entry fails the import); the import is skipped when the realm
# already exists (delete the realm + the import Job to re-run it).
# The Record Validation demo (Demo scenarios, Demo 2) is optional — its manifest
# deploy/demos/02-record-validation-demo.yaml needs the proxy's virtual cluster, so it
# deploys with the proxy (deploy order 6).
oc apply -f deploy/apicurio/01-subscription.yaml
oc -n kafka-registry wait --for=create installplan --all --timeout=300s
oc -n kafka-registry patch installplan $(oc -n kafka-registry get installplan -o jsonpath='{.items[0].metadata.name}') --type merge -p '{"spec":{"approved":true}}'
oc -n kafka-registry wait --for=create csv --all --timeout=300s
oc -n kafka-registry wait --for=jsonpath='{.status.phase}'=Succeeded csv --all --timeout=600s
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
source deploy/env.sh
envsubst < deploy/apicurio/03-registry.yaml | oc apply -f -
# Build the OIDC truststore with keytool in a JDK pod (Step 5.2 gotcha 1) — the registry
# pod mounts the keycloak-oidc-truststore Secret and cannot start without it:
oc -n kafka-registry run keytool-gen --image=registry.access.redhat.com/ubi8/openjdk-17 --command -- sleep 300
oc -n kafka-registry wait --for=create pod/keytool-gen --timeout=120s
oc -n kafka-registry wait --for=condition=Ready pod/keytool-gen --timeout=300s
oc get secret keycloak-tls-secret -n kafka-auth -o jsonpath='{.data.tls\.crt}' | base64 -d > /tmp/kc-ca.crt
oc -n kafka-registry cp /tmp/kc-ca.crt keytool-gen:/tmp/ca.crt
oc -n kafka-registry exec keytool-gen -- keytool -importcert -alias keycloak-ca -file /tmp/ca.crt \
  -keystore /tmp/ts.p12 -storetype PKCS12 -storepass changeit -noprompt
oc -n kafka-registry cp keytool-gen:/tmp/ts.p12 /tmp/ts.p12
oc -n kafka-registry create secret generic keycloak-oidc-truststore \
  --from-file=ca.p12=/tmp/ts.p12 --from-literal=ca.password=changeit
oc -n kafka-registry delete pod keytool-gen
oc -n kafka-registry wait --for=condition=Ready apicurioregistry3/apicurio-registry --timeout=900s
oc apply -f deploy/apicurio/04-netpol-dmz.yaml
# 5.4 — register the schema (contentId feeds the Demo 2 filter): see docs/05-apicurio-registry.md
# (writes need a Bearer token — the kafka-admin password grant in Step 5.4; anonymous reads only)

# Deploy order 6 — Proxy (Step 6): operator (OLM), proxy CRs, TLS, ACLs + filters.
oc apply -f deploy/proxy/00-proxy-operator-olm.yaml
oc get installplan -n openshift-operators
oc patch installplan <name> -n openshift-operators --type merge -p '{"spec":{"approved":true}}'
oc get csv -n openshift-operators | grep amq-streams-proxy   # Succeeded
# (upstream alternative: kroxylicious 0.25.1 release, Step 6.1)
# Server certificate for the TLS clusterIP ingresses (Step 6.8) — BEFORE the virtual
# clusters; a wildcard SAN covers both clusters' bootstrap/broker addresses:
openssl req -new -newkey rsa:2048 -nodes \
  -subj '/CN=kafka-proxy/O=Kafka Proxy/C=US' \
  -addext "subjectAltName=DNS:*.kafka-proxy.svc.cluster.local" \
  -keyout proxy-key.pem -x509 -days 365 -out proxy-cert.pem
oc -n kafka-proxy create secret tls proxy-tls --cert=proxy-cert.pem --key=proxy-key.pem
rm -f proxy-key.pem proxy-cert.pem
# The proxy's trustStrimziCaCertificate mounts the upstream clusters' CA Secrets from the
# PROXY namespace (not the Kafka CR's namespace) — copy them in, or the proxy runtime pod
# sticks in ContainerCreating ("secret not found"; verified live):
for N in 1 2; do
  oc -n kafka-cluster-${N} get secret kafka-cluster-${N}-cluster-ca-cert -o json | \
    python3 -c 'import json,sys
d = json.load(sys.stdin)
d["metadata"]["namespace"] = "kafka-proxy"
for k in ("uid","resourceVersion","creationTimestamp","managedFields","annotations","ownerReferences","finalizers"):
    d["metadata"].pop(k, None)
print(json.dumps(d))' | oc apply -f -
done
oc apply -f deploy/proxy/02-kafkaproxy.yaml -f deploy/proxy/03-kafkaproxyingress.yaml \
  -f deploy/proxy/04-kafkaservice.yaml -f deploy/proxy/05-virtualkafkacluster.yaml
# ACLs + OAuth validation filter — MUST go through envsubst: the oauth-validation filter's
# expectedIssuer embeds ${OPENSHIFT_APPS_DOMAIN}, and the operator's ${configmap:...}
# interpolation does not resolve it. Blanket envsubst is safe — ${configmap:...} is not a
# valid shell variable name and is left untouched (verified live; without it every SASL
# auth fails with "SASL failed : {\"status\":\"invalid_token\"}").
source deploy/env.sh
envsubst < deploy/proxy/06-kafka-acl.yaml | oc apply -f -
# The record-validation filter the virtual clusters reference (Demo 2 manifest) — needs the
# schema contentId registered in Step 5.4 (1 on a strictly-in-order fresh run; use the
# contentId returned by the registration if you registered anything else first):
export APICURIO_CONTENT_ID=1
envsubst < "deploy/demos/02-record-validation-demo.yaml" | oc apply -f -
oc -n kafka-proxy wait --for=create deployment/simple --timeout=300s   # created by the operator
oc -n kafka-proxy wait --for=condition=Available deployment/simple --timeout=600s
oc -n kafka-proxy wait --for=condition=ResolvedRefs=True virtualkafkacluster/kafka-cluster-1 --timeout=300s
oc -n kafka-proxy wait --for=condition=Accepted=True virtualkafkacluster/kafka-cluster-1 --timeout=300s
oc -n kafka-proxy wait --for=condition=ResolvedRefs=True virtualkafkacluster/kafka-cluster-2 --timeout=300s
oc -n kafka-proxy wait --for=condition=Accepted=True virtualkafkacluster/kafka-cluster-2 --timeout=300s
# 6.10 — smoke-test through the proxy: see docs/06-proxy.md (needs demo-client-oauth in kafka-proxy)

# Deploy order 7 — HTTP Bridge (Step 7): the bridge is a proxy client (DMZ).
oc -n kafka-http-bridge create secret generic kafka-bridge-oauth --from-literal=client-secret=kafka-bridge-secret
# The bridge trusts the proxy's ingress certificate — create its proxy-ca Secret in THIS
# namespace (the self-signed proxy-tls cert is its own CA):
oc get secret proxy-tls -n kafka-proxy -o jsonpath='{.data.tls\.crt}' | base64 -d > /tmp/proxy-ca.crt
oc -n kafka-http-bridge create secret generic proxy-ca --from-file=ca.crt=/tmp/proxy-ca.crt
oc apply -f deploy/bridge/
oc -n kafka-http-bridge wait --for=condition=Ready kafkabridge/my-bridge --timeout=600s

# Deploy order 8 — Console (Step 8): operator InstallPlan, then the CR and its TLS material.
oc apply -f deploy/console/01-console-operator.yaml
oc -n kafka-console wait --for=create installplan --all --timeout=300s
oc -n kafka-console patch installplan $(oc -n kafka-console get installplan -o jsonpath='{.items[0].metadata.name}') --type merge -p '{"spec":{"approved":true}}'
oc -n kafka-console wait --for=create csv --all --timeout=300s
oc -n kafka-console wait --for=jsonpath='{.status.phase}'=Succeeded csv --all --timeout=600s
# The console's Kafka connection client secret — the key must be the FULL property name
# (valuesFrom.secretRef turns every Secret key into a Kafka client property; Step 8.2 gotcha):
oc -n kafka-console create secret generic kafka-console-kafka-oauth \
  --from-literal=sasl.oauthbearer.client.credentials.client.secret=kafka-console-kafka-secret
# OIDC client secret + Keycloak CA trust store (Step 8.3):
oc -n kafka-console create secret generic kafka-console-oidc-secret --from-literal=client-secret=kafka-console-secret
oc get secret keycloak-tls-secret -n kafka-auth -o jsonpath='{.data.tls\.crt}' | base64 -d > /tmp/kc-ca.crt
oc -n kafka-console create configmap keycloak-ingress-ca --from-file=ca.crt=/tmp/kc-ca.crt
# Client TLS properties — every ConfigMap key is a full Kafka client property name:
# the proxy CA for the virtual-cluster entries, each cluster CA for the physical entries.
oc get secret proxy-tls -n kafka-proxy -o jsonpath='{.data.tls\.crt}' | base64 -d > /tmp/proxy-ca.crt
oc -n kafka-console create configmap console-kafka-tls --from-literal=ssl.truststore.type=PEM \
  --from-file=ssl.truststore.certificates=/tmp/proxy-ca.crt
for N in 1 2; do
  oc -n kafka-cluster-${N} get secret kafka-cluster-${N}-cluster-ca-cert -o jsonpath='{.data.ca\.crt}' | base64 -d > /tmp/kc${N}-ca.crt
  oc -n kafka-console create configmap console-kafka-cluster${N}-ca --from-literal=ssl.truststore.type=PEM \
    --from-file=ssl.truststore.certificates=/tmp/kc${N}-ca.crt
done
rm -f /tmp/kc-ca.crt /tmp/proxy-ca.crt /tmp/kc1-ca.crt /tmp/kc2-ca.crt /tmp/ts.p12
envsubst < deploy/console/02-kafka-console.yaml | oc apply -f -   # console hostname + OIDC
oc apply -f deploy/console/03-console-metrics.yaml
# Wait on the FULLY-QUALIFIED type — bare 'console' collides with the OpenShift Console CRD:
oc -n kafka-console wait --for=condition=Ready consoles.console.streamshub.github.com/kafka-console --timeout=600s
curl -sk -o /dev/null -w '%{http_code} -> %{redirect_url}\n' https://kafka-console.${OPENSHIFT_APPS_DOMAIN}   # 307 -> .../api/auth/signin

# Deploy order 9 — Camel clients (Step 9): Secrets, manifests, in-cluster builds.
# The kafka-producer/kafka-consumer namespaces do not exist yet on a fresh cluster (their
# manifests ship in deploy/camel/ below) — create them first, or the Secret creates fail
# with `namespaces "kafka-producer" not found` (verified live):
oc new-project kafka-producer
oc new-project kafka-consumer
oc -n kafka-producer create secret generic camel-producer-oauth --from-literal=client-secret=camel-producer-secret
oc -n kafka-consumer create secret generic camel-consumer-oauth --from-literal=client-secret=camel-consumer-secret
oc apply -f deploy/camel/producer/ -f deploy/camel/consumer/
# The client pods mount the proxy CA as a ConfigMap (Step 9.4 field notes) — without it
# they stick in ContainerCreating:
oc get secret proxy-tls -n kafka-proxy -o jsonpath='{.data.tls\.crt}' | base64 -d > /tmp/proxy-ca.crt
for NS in kafka-producer kafka-consumer; do
  oc apply -f - <<EOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: proxy-ca
  namespace: ${NS}
data:
  ca.crt: |
$(sed 's/^/    /' /tmp/proxy-ca.crt)
EOF
done
rm -f /tmp/proxy-ca.crt
oc -n kafka-producer start-build camel-kafka-producer --from-dir=deploy/camel/producer --follow
oc -n kafka-consumer start-build camel-kafka-consumer --from-dir=deploy/camel/consumer --follow
oc -n kafka-producer wait --for=condition=Available deployment/camel-kafka-producer --timeout=600s
oc -n kafka-consumer wait --for=condition=Available deployment/camel-kafka-consumer --timeout=600s

# Deploy order 10 — per-component monitoring integration (netpols, PodMonitors, alerts):
oc apply -f deploy/observability/01-metrics-netpols.yaml -f deploy/observability/02-kafka-podmonitor.yaml \
  -f deploy/observability/03-proxy-podmonitors.yaml -f deploy/observability/04-bridge-metrics.yaml \
  -f deploy/observability/05-kafka-stack-alerts.yaml
```

Optional — **Perses dashboards** (COO + monitoring UIPlugin, Technology Preview; see [Step 1.10](../docs/01-user-workload-monitoring.md#110--perses-dashboards-in-the-console-optional-technology-preview)):

```bash
oc apply -f deploy/observability/06-perses.yaml
oc apply -f deploy/observability/07-perses-dashboards.yaml -f deploy/observability/09-perses-strimzi-dashboards.yaml
oc create token perses-datasource -n kafka --duration=8760h
oc create secret generic user-workload-monitoring-secret \
  --from-literal=tokens=<token> -n kafka   # datasource proxy secret (`tokens` key)
```

> **OAuth credentials for the data path:** the `tls` listener requires SASL/OAUTHBEARER — the service-account clients in the `kafka` realm ([`deploy/keycloak/04-kafka-realm-import.yaml`](keycloak/04-kafka-realm-import.yaml)) are the workloads' credentials. The realm import does **not** create Kubernetes Secrets — create one per workload in its own namespace before deploying the clients:
>
> ```bash
> oc create secret generic kafka-bridge-oauth --from-literal=client-secret=kafka-bridge-secret -n kafka-http-bridge
> oc create secret generic camel-producer-oauth --from-literal=client-secret=camel-producer-secret -n kafka-producer
> oc create secret generic camel-consumer-oauth --from-literal=client-secret=camel-consumer-secret -n kafka-consumer
> # The console's key must be the FULL property name — valuesFrom.secretRef turns every
> # Secret key into a Kafka client property (Step 8.2 gotcha):
> oc create secret generic kafka-console-kafka-oauth --from-literal=sasl.oauthbearer.client.credentials.client.secret=kafka-console-kafka-secret -n kafka-console
> # demo-client-oauth in kafka-proxy — see docs/06-proxy.md Step 6.10 (smoke tests)
> ```

## Namespaces ship with each component

There is no central namespaces file — each component's first manifest contains
its own `Namespace`, so applying a component's directory is self-contained:

| Component manifest | Namespace created |
|---|---|
| `deploy/kafka/00-cluster-namespaces.yaml` | `kafka-cluster-1`, `kafka-cluster-2`, `kafka-http-bridge` |
| `deploy/kafka/00-cluster-operator-olm.yaml` | `kafka` |
| `deploy/keycloak/00-namespace.yaml` | `kafka-auth` |
| `deploy/keycloak/01-kafka-auth-operator.yaml` | (OperatorGroup + Subscription for the `kafka-auth` namespace created above) |
| `deploy/proxy/00-proxy-operator-olm.yaml` | `openshift-operators` (operator; upstream install path described in Step 6.1) |
| `deploy/bridge/01-kafka-bridge.yaml` | `kafka-http-bridge` |
| `deploy/bridge/02-bridge-netpol-dmz.yaml` | `api-gateway` (stand-in for the DMZ verification) |
| `deploy/mirrormaker2/00-namespace.yaml` | `kafka-mm2` |
| `deploy/mirrormaker2/01-mirrormaker2.yaml` | (the KafkaMirrorMaker2 for the `kafka-mm2` namespace created above) |
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
| `deploy/observability/06–07-perses*.yaml` + `09-perses-strimzi-dashboards.yaml` | Perses console dashboards (optional, Tech Preview): COO subscription + monitoring `UIPlugin`, `PersesDatasource` (UWM Thanos Querier) + `cluster-monitoring-view` RBAC, and the full Strimzi Grafana dashboard set translated to `PersesDashboard`s (kafka project) | [1.10](../docs/01-user-workload-monitoring.md#110--perses-dashboards-in-the-console-optional-technology-preview) |
| `deploy/custom-filter/` | Custom filter plugin: Dockerfile + image build + `KafkaProtocolFilter` (requires the local Maven filter project from [Step 10.1](../docs/10-custom-filter.md#101--generate-and-test-a-filter-project)) | [10](../docs/10-custom-filter.md) |
| `deploy/demos/` | Demo manifests (Step 11, optional): **Record Validation demo** (Demo 2 — topic + filter wiring, `02-record-validation-demo.yaml`; its filter deploys with the proxy), proxy filters (authorization, entity isolation, token validation), multi-tenant virtual clusters, MM2 failover variant, Record Encryption with a dev Vault | [11](../docs/11-demos.md) |

## Templating: `OPENSHIFT_APPS_DOMAIN`

Manifests that embed cluster-specific hostnames (Route hostnames, the Keycloak
issuer/JWKS URLs, the console OIDC server) use the `${OPENSHIFT_APPS_DOMAIN}`
variable — the cluster's default Ingress Controller domain (the suffix of every
Route hostname). Source `env.sh` to set it from the live cluster, then apply
the affected files through `envsubst`:

```bash
source deploy/env.sh

envsubst < deploy/keycloak/04-kafka-realm-import.yaml | oc apply -f -   # console redirect URI
envsubst < deploy/kafka/02-kafka.yaml | oc apply -f -                   # Kafka listener hostname
envsubst < deploy/apicurio/03-registry.yaml | oc apply -f -             # registry Routes + Keycloak auth
envsubst < deploy/proxy/06-kafka-acl.yaml | oc apply -f -               # oauth-validation expectedIssuer
envsubst < deploy/console/02-kafka-console.yaml | oc apply -f -  # console hostname + OIDC
```

Files without `${OPENSHIFT_APPS_DOMAIN}` can be applied directly with
`oc apply -f <file>`. (`envsubst` leaves other `${...}` sequences — such as
Strimzi config-provider references or the proxy's `${configmap:...}`
interpolation ref — untouched: they are not valid shell variable names, so
blanket `envsubst` is safe even on files that mix both, like
`deploy/proxy/06-kafka-acl.yaml`. Applying that file **raw** leaves the domain
literal in the filter's `expectedIssuer` and every SASL auth fails with
`SASL failed : {"status":"invalid_token"}` — verified live.)

`deploy/demos/02-record-validation-demo.yaml` additionally uses
`${APICURIO_CONTENT_ID}` — the schema's `contentId` from the registry, obtained
in [Step 5.4](../docs/05-apicurio-registry.md#54--register-a-schema):

```bash
export APICURIO_CONTENT_ID=1
envsubst < deploy/demos/02-record-validation-demo.yaml | oc apply -f -
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
- **Quota administration** (`kafka-configs.sh` per-principal rate limits,
  [Step 3.5](../docs/03-kafka-cluster.md#35--quotas-per-principal-broker-enforced)) —
  dynamic broker config keyed on the OAuth principals, not declared Kubernetes state.

Placeholders in manifest files:

- `<KEYCLOAK_HOST>` — the externally reachable hostname of the `keycloak` Keycloak route (filled in after [Step 2.3](../docs/02-keycloak.md#23--tls-certificate-and-keycloak-deployment) — see [openshift-setup.md, Step 2](../docs/02-keycloak.md#step-2-install-red-hat-build-of-keycloak-kafka-auth-namespace)). Derived as `keycloak-ingress-kafka-auth.${OPENSHIFT_APPS_DOMAIN}`; used by the console OIDC configuration.

Secrets (Keycloak TLS material, the console OIDC client secret) are created with `oc create` commands from the guide — they are deliberately NOT in git.
