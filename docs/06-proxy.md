# Step 6: Deploy the Streams for Apache Kafka Proxy (kafka-proxy namespace)

> [↑ Setup overview](../openshift-setup.md) · prev: [Step 5: Apicurio Registry](05-apicurio-registry.md) · next: [Step 7: HTTP Bridge](07-http-bridge.md)

This step covers the whole proxy deployment in subsections 6.1–6.10: install the
operator, understand the custom resources, create the proxy / ingress / backend
cluster / virtual cluster, wire TLS and DNS, and verify the virtual cluster end to end.

> **Placement:** the proxy runs in the **DMZ** (`kafka-proxy`) — it is the only door into the
> Kafka clusters, which sit in the **secure backend** (`kafka-cluster-1`/`kafka-cluster-2`) and
> accept Kafka-protocol connections from the proxy namespace only ([Step 3.4](03-kafka-cluster.md#34--restrict-kafka-ingress-to-the-proxy-namespace-dmz)).

> 🎬 The full client → proxy → broker communication path — both TLS sessions (client→proxy terminated at the proxy, proxy→brokers terminated at the brokers), the relayed SASL/OAUTHBEARER exchange, and who trusts which CA — is diagrammed in use case [§2 Stable Client Endpoints & TLS Termination](../README.md#2-stable-client-endpoints--tls-termination).

## 6.1 — Install the Proxy Operator

### Recommended: OperatorHub/OLM — the Red Hat build (as deployed here)

The deployed operator is the **officially supported Red Hat build** (`amq-streams-proxy`, Streams for Apache Kafka Proxy), installed via OperatorHub/OLM and version-locked to the Streams 3.2 line — the same style as the Streams cluster operator subscription ([Step 3.1](03-kafka-cluster.md#31--install-the-cluster-operator)). [`deploy/proxy/00-proxy-operator-olm.yaml`](../deploy/proxy/00-proxy-operator-olm.yaml) carries the Subscription (channel `amq-streams-3.2.x`, `startingCSV` pinned, `installPlanApproval: Manual`):

> **Install mode (verified live):** this operator supports **only the AllNamespaces InstallMode** — OwnNamespace/SingleNamespace/MultiNamespace are unsupported (a subscription in `kafka-proxy` with a targeted OperatorGroup fails with "OwnNamespace InstallModeType not supported"). The subscription therefore lives in `openshift-operators` (pre-existing global OperatorGroup — none needed here), while the proxy CRs live in `kafka-proxy` (`deploy/proxy/02–06`); the operator watches all namespaces.

```bash
# The kafka-proxy namespace — nothing creates it (the OLM subscription lives in
# openshift-operators and cannot create namespaces), and the proxy CRs (Steps
# 6.3–6.7) as well as Step 3.4's DMZ verification need it:
oc apply -f deploy/proxy/00-namespace.yaml

oc apply -f deploy/proxy/00-proxy-operator-olm.yaml

oc get installplan -n openshift-operators
oc patch installplan <name> -n openshift-operators --type merge \
  -p '{"spec":{"approved":true}}'

oc get csv -n openshift-operators | grep amq-streams-proxy   # Succeeded
```

> **Coexistence with an older Streams operator:** the proxy operator is an **independent OLM subscription** — it neither manages nor conflicts with a Streams cluster operator of a different version already on the cluster (e.g., `amq-streams` 3.0.x): separate operators, separate CRD families (`KafkaProxy`/`KafkaService`/`VirtualKafkaCluster` vs `Kafka`/`KafkaTopic`), no shared watches. This stack's `KafkaService` also uses **explicit `bootstrapServers`** — no `strimziKafkaRef` ([6.5](#65--define-the-backend-cluster-kafkaservice)) — so there is no version coupling to the Kafka CRs at all. The caveats: the proxy is TP-pinned to 3.2 testing, and a proxied **Kafka 3.0** cluster's protocol vintage through the 3.2 proxy is untested territory (Kafka 3.0.x is also past its maintenance window) — verify the data path end-to-end in the PoC, and plan the Streams upgrade to 3.2 regardless.

### Alternative 2: Red Hat subscription artifacts

Download the release archive `streams-3.2-ocp-install-examples.zip` from the [Streams for Apache Kafka software downloads page](https://access.redhat.com/jbossnetwork/restricted/listSoftware.html?downloadType=distributions&product=jboss.amq.streams) and extract it — `install/` contains the YAML manifests for the operator, `examples/` example custom resources for deploying a proxy:

```bash
unzip streams-3.2-ocp-install-examples.zip -d streams-kafka-proxy
cd streams-kafka-proxy

oc new-project kafka-proxy

# Linux (macOS: sed -i '')
sed -i 's/namespace: .*/namespace: kafka-proxy/' install/*.yaml

oc create -f install
```

> The Red Hat build names the operator Deployment `kroxylicious-operator` — keep that name in the steps that follow, or rename it to `kafka-proxy` to match this guide.

### Alternative: OperatorHub (consolidated YAML)

If you prefer an OLM-managed install, consolidate Namespace, OperatorGroup, and Subscription into one file and apply it with `oc apply`. First discover the exact package name:

```bash
oc get packagemanifests -n openshift-marketplace | grep -i "proxy"
```

```yaml
# kafka-proxy-operator-subscription.yaml
---
apiVersion: v1
kind: Namespace
metadata:
  name: kafka-proxy
---
apiVersion: operators.coreos.com/v1
kind: OperatorGroup
metadata:
  name: proxy-operator-group
  namespace: kafka-proxy
spec: {}   # global OperatorGroup — the operator watches and manages proxies cluster-wide
---
apiVersion: operators.coreos.com/v1alpha1
kind: Subscription
metadata:
  name: streams-kafka-proxy-operator
  namespace: kafka-proxy
spec:
  channel: alpha                # or amq-streams-X.Y.x for version-pinned updates
  name: <package-name>          # from the packagemanifests output above
  source: redhat-operators
  sourceNamespace: openshift-marketplace
  installPlanApproval: Manual   # auto-upgrades on the default alpha channel skip pre-upgrade steps
```

```bash
oc apply -f kafka-proxy-operator-subscription.yaml

oc get subscriptions -n kafka-proxy
# Gate on creation first — a condition wait on a not-yet-created CSV blocks until
# timeout instead of failing fast:
oc wait --for=create csv --all -n kafka-proxy --timeout=300s
oc wait --for=jsonpath='{.status.phase}'=Succeeded csv --all -n kafka-proxy --timeout=300s
```

The rest of this guide works identically regardless of install method.

## 6.2 — Understand the Custom Resources

The operator consumes these custom resources (API version `kroxylicious.io/v1alpha1`) and generates the OpenShift `Deployment`, `Service`, and `ConfigMap` resources that run the proxy:

| Resource | Purpose |
|---|---|
| `KafkaProxy` | An instance of the proxy; the top-level resource linking everything together |
| `KafkaProxyIngress` | How a virtual cluster is exposed on the network |
| `KafkaService` | The backend (upstream) Kafka cluster |
| `VirtualKafkaCluster` | The logical cluster presented to clients |
| `KafkaProtocolFilter` | Filter applied to traffic on a virtual cluster |
| `Secret` / `ConfigMap` | Referenced by `KafkaService` / `KafkaProtocolFilter` for TLS certs, tokens, and CA material |

> The API is deliberately decomposed (similar to the Kubernetes Gateway API), so you can use RBAC to split responsibility — e.g., network teams own `KafkaProxy`/`KafkaProxyIngress`, application teams own `VirtualKafkaCluster`/`KafkaService`/`KafkaProtocolFilter`.

## 6.3 — Create the KafkaProxy Resource

A `KafkaProxy` is the top-level resource linking everything together — an empty `spec` creates a proxy with default configuration. The ready-to-apply manifest is [`deploy/proxy/02-kafkaproxy.yaml`](../deploy/proxy/02-kafkaproxy.yaml) (`name: simple`, `namespace: kafka-proxy`, `spec: {}`).

## 6.4 — Choose and Create an Ingress

A `KafkaProxyIngress` defines how clients reach the virtual cluster. It belongs to one `KafkaProxy` (via `proxyRef`) but can be shared by multiple virtual clusters.

| Ingress Type | Use case | Transport | DNS changes needed? |
|---|---|---|---|
| `clusterIP` | On-cluster applications | TCP / TLS | No |
| `loadBalancer` | Off-cluster applications | TLS only | Yes |
| `openShiftRoute` | Off-cluster applications (OpenShift only) | TLS only | No — hostnames assigned by OpenShift |

> ℹ️ **The platform-side implementation of the off-cluster ingresses is out of scope for this guide** — provisioning the load balancer or router exposure is platform (and provider) dependent (on AWS, the cloud provider fronts the proxy with a Network Load Balancer and DNS lives in Route 53). This guide covers the in-cluster resources: the `KafkaProxyIngress` manifests below, the DNS mappings ([Step 6.9](#69--dns-for-off-cluster-ingress)), and the certificate requirements ([Step 6.8](#68--tls-certificates)); the off-cluster plumbing is up to your platform team.

**clusterIP (TCP or TLS)** — uses a `ClusterIP` Service; for on-cluster clients. The ready-to-apply manifest is [`deploy/proxy/03-kafkaproxyingress.yaml`](../deploy/proxy/03-kafkaproxyingress.yaml) — two `clusterIP` ingresses with `protocol: TLS` (the proxy terminates TLS on ONE shared SNI port; SNI identifies the virtual cluster and broker).

**loadBalancer (TLS only)** — uses a `LoadBalancer` Service; the proxy uses SNI to route, so every bootstrap and broker address must be unique in DNS. The `$(nodeId)` token is required in the broker pattern; `$(virtualClusterName)` may be used when sharing the ingress across virtual clusters:

```yaml
kind: KafkaProxyIngress
apiVersion: kroxylicious.io/v1alpha1
metadata:
  namespace: kafka-proxy
  name: load-balancer
spec:
  proxyRef:
    name: simple
  loadBalancer:
    bootstrapAddress: "$(virtualClusterName).kafkaproxy.example.com"
    advertisedBrokerAddressPattern: "broker-$(nodeId).$(virtualClusterName).kafkaproxy.example.com"
```

**openShiftRoute (TLS only)** — uses OpenShift `Route` resources; bootstrap and broker hostnames are generated automatically as `<virtualClusterName>-bootstrap.<ingressDomain>:443` and `<virtualClusterName>-<nodeId>.<ingressDomain>:443`:

```yaml
kind: KafkaProxyIngress
apiVersion: kroxylicious.io/v1alpha1
metadata:
  namespace: kafka-proxy
  name: openshift-route
spec:
  proxyRef:
    name: simple
  openShiftRoute: {}
```

> TLS is recommended for production connections. `loadBalancer` and `openShiftRoute` support TLS only.

## 6.5 — Define the Backend Cluster (KafkaService)

The `KafkaService` describes the upstream Kafka cluster. Declare the full range of broker node IDs. The ready-to-apply manifest is [`deploy/proxy/04-kafkaservice.yaml`](../deploy/proxy/04-kafkaservice.yaml) — one `KafkaService` per backend cluster (`kafka-cluster-1` and `kafka-cluster-2`), each using explicit `bootstrapServers` (the Kafka cluster's bootstrap Service over the `tls` listener, 9093) and `tls.trustAnchorRef` pointing at the Kafka cluster CA Secret copied into the proxy namespace:

```bash
oc apply -f deploy/proxy/04-kafkaservice.yaml
```

Field notes (full manifests in the deploy file):

- `spec.bootstrapServers` — the Kafka cluster's bootstrap Service address (`kafka-cluster-1-kafka-bootstrap.kafka-cluster-1.svc.cluster.local:9093`; specify either `bootstrapServers` or `strimziKafkaRef`, not both).
- `spec.tls.trustAnchorRef` — a reference to the cluster CA bundle: `name: kafka-cluster-1-cluster-ca-cert`, `key: ca.crt`, `kind: Secret`, `storeType: PEM` (the Kafka cluster CA Secret, copied into `kafka-proxy`).
- **Verified live (Red Hat operator)**: the Red Hat build does **not** resolve cross-namespace `strimziKafkaRef` referents — it looks up the `Kafka` (and its CA) in the `KafkaService`'s own namespace only and reports `ResolvedRefs=False: referenced Kafka resource not found`. Use the explicit `bootstrapServers` + copied-CA pattern above.
- `nodeIdRanges` must cover **all** broker node IDs in the target cluster (`start` and `end` inclusive) — `0`–`2` for the cluster deployed in [Step 3](03-kafka-cluster.md#step-3-deploy-the-kafka-cluster-streams-for-apache-kafka).

## 6.6 — (Optional) Add a Filter

A `KafkaProtocolFilter` is reusable across virtual clusters in the same namespace. `spec.type` names the filter implementation and `spec.configTemplate` carries the configuration (subject to operator interpolation):

```yaml
kind: KafkaProtocolFilter
apiVersion: kroxylicious.io/v1alpha1
metadata:
  namespace: kafka-proxy
  name: my-filter
spec:
  type: RecordValidation
  configTemplate:
    rules:
      - topicNames:
          - my-topic
        valueRule:
          syntacticallyCorrectJson:
            allowNulls: false
            allowEmpty: false
```

Start with **no filters** to validate basic connectivity, then add one at a time. See the [use case catalog](../README.md#use-case-catalog) for Record Encryption and Record Validation configuration.

### ACLs on the proxy layer (as deployed here)

The **`Authorization` filter** enforces ACL rules on client requests **before the brokers** (deny-by-default, on top of any broker-side checks — both must allow). Two filters make it work, defined in [`deploy/proxy/06-kafka-acl.yaml`](../deploy/proxy/06-kafka-acl.yaml):

- **`SaslInspection`** — authenticates the client (SASL/OAUTHBEARER passthrough) and puts the end-client principal in front of the Authorization filter. Chain order matters: without it the Authorization filter sees no subject and denies **everything**. It derives the principal from the JWT `sub` claim (a UUID), so the ACL rules must match that form.
- **`Authorization`** — `type: Authorization`, `authorizer: AclAuthorizerService`, `authorizerConfig.aclFile: ${configmap:kafka-acl-rules:acl-rules.txt}`. An **experimental filter** (the proxy logs a warning). Rules live in the `kafka-acl-rules` ConfigMap: `deny` rules first, then `allow`, ending with `otherwise deny;` — the grammar requires a resource predicate (`to * Topic with name like "*";`; a bare `to * Topic;` fails with `mismatched input ';' expecting 'with'`, verified live), `in {"a","b"}` for user sets, and the keyword `ALL` is not supported.

The deployed least-privilege policy: infrastructure clients (`service-account-kafka-bridge`, `service-account-kafka-console-kafka`, `kafka-admin`, plus the JWT `sub` UUIDs SaslInspection derives) get full access; `service-account-camel-producer` gets `WRITE` on `topic-A`; `service-account-camel-consumer` gets `READ` on `topic-A` + its consumer group; demo data-path users get scoped grants (`kafka-user-a`: `{READ, WRITE}` on `topic-A` + `READ` on group `kafka-user-a-group`; `kafka-user-b`: `READ` on `topic-A` + `READ` on group `kafka-user-b-group`; `kafka-user-c`: `{READ, WRITE}` on `topic-C` + `READ` on group `kafka-user-c-group` — a consuming client needs the **group** ACL too, verified live with password-grant tokens: user-a produces to `topic-A` and user-c to `topic-C`, both users consume via their groups, and cross-topic produces (user-b → `topic-A`, anyone → `topic-B`) are rejected with `TopicAuthorizationException`); **everything else is denied** (including `demo-client`). Clients connecting directly to the brokers (MM2, internal listeners) are unaffected. Wire the filters with `filterRefs` on both virtual clusters (`record-validation` additionally on `kafka-cluster-1`); a malformed rules file or unknown config field crash-loops the proxy (`CrashLoopBackOff`) until corrected.

## 6.7 — Create the VirtualKafkaCluster

The `VirtualKafkaCluster` wires everything together. All referenced resources must be in the same namespace. Filter order matters: client requests pass through filters from index `0` to `n`; broker responses pass through in reverse order. An empty/omitted `filterRefs` passes traffic through unchanged.

The ready-to-apply manifest is [`deploy/proxy/05-virtualkafkacluster.yaml`](../deploy/proxy/05-virtualkafkacluster.yaml) — **one virtual cluster per backend cluster** (`kafka-cluster-1` and `kafka-cluster-2`), each wiring `proxyRef: simple`, its `targetKafkaServiceRef`, its own `clusterIP` ingress (`cluster-ip-1` / `cluster-ip-2` with a shared TLS `certificateRef: proxy-tls`, applied in [Step 6.8](#68--tls-certificates)), and `filterRefs` — `[sasl-inspection, record-validation, kafka-authorization]` on `kafka-cluster-1` and `[sasl-inspection, kafka-authorization]` on `kafka-cluster-2` (`record-validation` created in [Step 5](05-apicurio-registry.md#step-5-deploy-apicurio-registry-and-validate-records-record-validation); if you are running strictly in order, remove those refs until their steps):

```bash
oc apply -f deploy/proxy/02-kafkaproxy.yaml -f deploy/proxy/03-kafkaproxyingress.yaml \
  -f deploy/proxy/04-kafkaservice.yaml -f deploy/proxy/05-virtualkafkacluster.yaml
```

## 6.8 — TLS Certificates

TLS ingress types need a server certificate provided as a `Secret` of type `kubernetes.io/tls`:

```bash
oc apply -f - <<EOF
apiVersion: v1
kind: Secret
metadata:
  name: <secret-name>
  namespace: <namespace>
type: kubernetes.io/tls
stringData:
  tls.crt: |
$(sed 's/^/    /' <path/to/cert/file>)
  tls.key: |
$(sed 's/^/    /' <path/to/key/file>)
EOF
```

Certificate requirements:

- **PKCS#8 encoded PEM** private key, not password protected.
- **SANs must cover every address** clients will use (or use a wildcard certificate).
- Signed by a CA trusted by the **connecting applications** (on-cluster CA for `clusterIP`, off-cluster/public CA for `loadBalancer` and `openShiftRoute`).

Which SANs are needed per ingress type (assuming virtual cluster `kafka-cluster-1` with ingress `cluster-ip-1`, brokers 0–2):

| Ingress | SANs |
|---|---|
| `clusterIP` | `kafka-cluster-1-cluster-ip-1-bootstrap.<namespace>.svc.cluster.local` plus `kafka-cluster-1-cluster-ip-1-<nodeId>.<namespace>.svc.cluster.local` for each broker |
| `loadBalancer` | `mycluster.kafkaproxy.example.com` plus `broker-<nodeId>.mycluster.kafkaproxy.example.com` (derived from your `bootstrapAddress` / `advertisedBrokerAddressPattern`) |
| `openShiftRoute` | `mycluster-bootstrap.<ingressDomain>` plus `mycluster-<nodeId>.<ingressDomain>` — look up the ingress domain first: |

```bash
oc get ingresscontrollers.operator.openshift.io -n openshift-ingress-operator default -o=jsonpath='{.status.domain}'
```

> Ingress Controller sharding can result in different ingress domains per address — include all of them as SANs.
>
> Concretely, for this guide's `kafka-proxy` namespace with brokers 0–2, the `clusterIP` SANs are:
>
> ```text
> kafka-cluster-1-cluster-ip-1-bootstrap.kafka-proxy.svc.cluster.local
> kafka-cluster-1-cluster-ip-1-0.kafka-proxy.svc.cluster.local
> kafka-cluster-1-cluster-ip-1-1.kafka-proxy.svc.cluster.local
> kafka-cluster-1-cluster-ip-1-2.kafka-proxy.svc.cluster.local
> ```

## 6.9 — DNS for Off-Cluster Ingress

**`openShiftRoute`**: nothing to do — OpenShift assigns the hostnames.

**`loadBalancer`**: bootstrap and broker names must resolve to the load balancer's external address:

1. Discover the external address:

   ```bash
   oc get virtualkafkacluster -n <namespace> <virtual-cluster-name> \
     -o=jsonpath='{.status.ingresses[?(@.name == "<ingress-name>")].loadBalancerIngressPoints}'
   ```

2. Create DNS mappings (example for `my-cluster` with brokers 0–2):

   ```text
   my-cluster.kafkaproxy.example.com            => <external address>
   broker-0.my-cluster.kafkaproxy.example.com   => <external address>
   broker-1.my-cluster.kafkaproxy.example.com   => <external address>
   broker-2.my-cluster.kafkaproxy.example.com   => <external address>
   ```

3. Verify resolution from the application network:

   ```bash
   nslookup my-cluster.kafkaproxy.example.com
   nslookup broker-0.my-cluster.kafkaproxy.example.com
   ```

## 6.10 — Apply and Verify

1. Apply the resources (order doesn't strictly matter — the operator reconciles references). The ready-to-apply manifests are in [`deploy/proxy/`](../deploy/proxy) (Steps 6.3 and 6.7 above):

   ```bash
   oc apply -f deploy/proxy/02-kafkaproxy.yaml -f deploy/proxy/03-kafkaproxyingress.yaml \
     -f deploy/proxy/04-kafkaservice.yaml -f deploy/proxy/05-virtualkafkacluster.yaml
   ```

2. **Wait for the operator to reconcile the resources.** The operator reports status conditions on the custom resources:

   - `ResolvedRefs=True` — all referenced resources exist (`KafkaProxy`, `KafkaProxyIngress`, `Secret`, ...)
   - `Accepted=True` — the configuration was successfully applied to the proxy

   ```bash
   oc wait --for=condition=ResolvedRefs=True virtualkafkacluster/kafka-cluster-1 \
     -n kafka-proxy --timeout=120s

   oc wait --for=condition=Accepted=True virtualkafkacluster/kafka-cluster-1 \
     -n kafka-proxy --timeout=120s

   oc wait --for=condition=ResolvedRefs=True kafkaproxyingress/cluster-ip-1 \
     -n kafka-proxy --timeout=120s
   ```

   If these time out, inspect the conditions for `reason` and `message` details:

   ```bash
   oc get virtualkafkacluster kafka-cluster-1 -n kafka-proxy \
     -o jsonpath='{.status.conditions}' | jq
   ```

3. **Wait for the proxy pods to be ready.** The operator creates a `Deployment` named after the `KafkaProxy` resource (here: `simple`) — gate on creation first, since the operator (not you) creates it:

   ```bash
   oc wait --for=create deployment/simple -n kafka-proxy --timeout=300s
   oc wait --for=condition=Available deployment/simple -n kafka-proxy --timeout=120s
   oc get pods -n kafka-proxy
   ```

4. Connect a client using the bootstrap address for your ingress type:
   - `clusterIP`: `<virtual-cluster>-<ingress>-bootstrap.<namespace>.svc.cluster.local` — the port is **operator-allocated**; get it from the generated Service or from `oc get virtualkafkacluster <name> -o jsonpath='{.status.ingresses[].bootstrapServer}'` (9292 with the operator versions this guide installs):

     ```bash
     oc get services -n kafka-proxy
     ```

   - `loadBalancer` / `openShiftRoute`: the bootstrap address from your DNS/Route configuration.

5. **Smoke-test end-to-end (plain TCP)** from inside the cluster with a Kafka CLI pod (for ingresses **without** TLS — for TLS ingresses use step 10). The proxied listener authenticates with SASL/OAUTHBEARER (the only mechanism on the `tls` listener — see Step 3.2), so even a plain-TCP client must authenticate with a token — the proxy relays the SASL/OAUTHBEARER exchange to the brokers. `kcat`/librdkafka cannot fetch OAuth tokens on its own, so use the Kafka CLI with the native OAuth handler:

   ```bash
   # 1. Create the Secret for the demo-client (the realm import does not create Secrets)
   oc create secret generic demo-client-oauth \
     --from-literal=client-secret=demo-client-secret -n kafka-proxy

   # 2. Run a Kafka CLI pod and produce through the proxy
   oc run kafka-cli --rm -i --restart=Never --image=quay.io/strimzi/kafka:latest-kafka-4.2.2 -n kafka-proxy -- \
     bash -c '
       cat > /tmp/client.properties <<EOF
security.protocol=SASL_PLAINTEXT
sasl.mechanism=OAUTHBEARER
sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;
sasl.login.callback.handler.class=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler
sasl.oauthbearer.client.credentials.client.id=demo-client
sasl.oauthbearer.client.credentials.client.secret=$OAUTH_CLIENT_SECRET
sasl.oauthbearer.token.endpoint.url=http://keycloak-http.kafka-auth.svc.cluster.local:8080/realms/kafka/protocol/openid-connect/token
EOF
       echo "hello through the proxy (SASL_PLAINTEXT)" | bin/kafka-console-producer.sh \
         --bootstrap-server kafka-cluster-1-cluster-ip-1-bootstrap.kafka-proxy.svc.cluster.local:9292 \
         --topic topic-A --command-config /tmp/client.properties
       bin/kafka-console-consumer.sh \
         --bootstrap-server kafka-cluster-1-cluster-ip-1-bootstrap.kafka-proxy.svc.cluster.local:9292 \
         --topic topic-A --from-beginning --timeout-ms 10000 \
         --command-config /tmp/client.properties
     '
   ```

   The pod needs the `OAUTH_CLIENT_SECRET` env var from the Secret and the Kafka 4.x http-token-endpoint allowlist — create the pod with `--overrides`. The securityContext is required in PodSecurity-`restricted` namespaces (and the image must be repeated inside the overrides):

   ```bash
   oc run kafka-cli --rm -i --restart=Never --image=quay.io/strimzi/kafka:latest-kafka-4.2.2 -n kafka-proxy \
     --overrides='{
       "spec": {
         "securityContext": {"seccompProfile": {"type": "RuntimeDefault"}, "runAsNonRoot": true},
         "containers": [{
           "name": "kafka-cli",
           "image": "quay.io/strimzi/kafka:latest-kafka-4.2.2",
           "env": [
             {"name": "OAUTH_CLIENT_SECRET", "valueFrom": {"secretKeyRef": {"name": "demo-client-oauth", "key": "client-secret"}}},
             {"name": "KAFKA_OPTS", "value": "-Dorg.apache.kafka.sasl.oauthbearer.allowed.urls=http://keycloak-http.kafka-auth.svc.cluster.local:8080/realms/kafka/protocol/openid-connect/token"}
           ],
           "securityContext": {"allowPrivilegeEscalation": false, "capabilities": {"drop": ["ALL"]}, "runAsNonRoot": true}
         }]
       }
     }' -- <command as above>
   ```

6. **Smoke-test end-to-end (TLS variant)** — for TLS ingresses (`clusterIP` with `protocol: TLS`, `loadBalancer`, or `openShiftRoute`), the client must trust the CA that signed the proxy's server certificate **and** authenticate with OAUTHBEARER. Extract the CA, make it available to the test pod via a ConfigMap, and point the CLI at both:

```bash
# 1. Extract the CA certificate (from the Secret/ConfigMap holding the signing CA)
oc get secret <ca-secret> -n kafka-proxy -o jsonpath='{.data.ca\.crt}' | base64 -d > ca.crt

# 2. Provide it to the test pod as a ConfigMap (idempotent via oc apply)
oc apply -f - <<EOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: proxy-ca
  namespace: kafka-proxy
data:
  ca.crt: |
$(sed 's/^/    /' ca.crt)
EOF

# 3. Run the CLI with the CA mounted, TLS, and OAUTHBEARER enabled
oc run kafka-cli --rm -i --restart=Never --image=quay.io/strimzi/kafka:latest-kafka-4.2.2 -n kafka-proxy \
  --overrides='{
    "spec": {
      "securityContext": {"seccompProfile": {"type": "RuntimeDefault"}, "runAsNonRoot": true},
      "containers": [{
        "name": "kafka-cli",
        "image": "quay.io/strimzi/kafka:latest-kafka-4.2.2",
        "env": [
          {"name": "OAUTH_CLIENT_SECRET", "valueFrom": {"secretKeyRef": {"name": "demo-client-oauth", "key": "client-secret"}}},
          {"name": "KAFKA_OPTS", "value": "-Dorg.apache.kafka.sasl.oauthbearer.allowed.urls=http://keycloak-http.kafka-auth.svc.cluster.local:8080/realms/kafka/protocol/openid-connect/token"}
        ],
        "securityContext": {"allowPrivilegeEscalation": false, "capabilities": {"drop": ["ALL"]}, "runAsNonRoot": true},
        "volumeMounts": [{"name": "proxy-ca", "mountPath": "/etc/proxy-ca", "readOnly": true}]
      }],
      "volumes": [{"name": "proxy-ca", "configMap": {"name": "proxy-ca"}}]
    }
  }' -- bash -c '
    cat > /tmp/client.properties <<EOF
security.protocol=SASL_SSL
sasl.mechanism=OAUTHBEARER
sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;
sasl.login.callback.handler.class=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler
sasl.oauthbearer.client.credentials.client.id=demo-client
sasl.oauthbearer.client.credentials.client.secret=$OAUTH_CLIENT_SECRET
sasl.oauthbearer.token.endpoint.url=http://keycloak-http.kafka-auth.svc.cluster.local:8080/realms/kafka/protocol/openid-connect/token
ssl.truststore.location=/etc/proxy-ca/ca.crt
ssl.truststore.type=PEM
EOF
    echo "hello through the proxy (TLS)" | bin/kafka-console-producer.sh \
      --bootstrap-server kafka-cluster-1-cluster-ip-1-bootstrap.kafka-proxy.svc.cluster.local:9292 \
      --topic topic-A --command-config /tmp/client.properties
  '
```

   Notes for the TLS variant:

   - Connect using the **SAN name from the server certificate** (the generated bootstrap hostname) so SNI and certificate validation succeed — never an IP address.
   - The proxy relays the client's SASL/OAUTHBEARER exchange to the brokers (SASL passthrough) — the broker validates the JWT against Keycloak's JWKS, so the token must come from a client in the `kafka` realm.
   - Kafka 4.x rejects **http** token endpoints unless allowlisted — the `KAFKA_OPTS` env var above sets `org.apache.kafka.sasl.oauthbearer.allowed.urls`. Using the Keycloak **https** route instead removes the need for it (but the pod must then trust Keycloak's CA).
   - `openShiftRoute` bootstrap endpoints use port **443**.
   - If the ingress uses client-certificate (mutual TLS) authentication, add `ssl.keystore.location`, `ssl.keystore.password`, and `ssl.key.password` as required.

7. Continue with [Getting Started](../README.md#getting-started-on-openshift) to add filters.

## 6.11 — (Optional) Proxying an external Kafka cluster

A `KafkaService` has nothing cluster-specific about it — it is just `bootstrapServers` + TLS trust + node-ID range (see [6.5](#65--define-the-backend-cluster-kafkaservice)). The same decoupling that lets Demo 9 stack three virtual clusters over two physical backends lets you point a **`KafkaService` at a Kafka cluster outside this OpenShift cluster** — Amazon MSK, Confluent Cloud, another OCP cluster, a partner's cluster — and front it with a virtual cluster exactly like the in-cluster ones:

```yaml
kind: KafkaService
apiVersion: kroxylicious.io/v1alpha1
metadata:
  namespace: kafka-proxy
  name: external-cluster
spec:
  bootstrapServers: my-cluster.kafka.external.example.com:9094   # the external cluster's bootstrap
  tls:
    trustAnchorRef:            # the EXTERNAL cluster's CA — a public CA (Confluent Cloud, MSK
      name: external-cluster-ca # with public certs) may not need an explicit anchor at all
      key: ca.crt
      kind: Secret
      storeType: PEM
  nodeIdRanges:
    - name: brokers            # must match the external cluster's broker IDs (e.g. MSK: 1–3)
      start: 1
      end: 3
```

Reference it from a new `VirtualKafkaCluster` ([6.7](#67--create-the-virtualkafkacluster)) with its own ingress — clusterIP for in-cluster clients, SNI/TLS for off-cluster ([6.4](#64--choose-and-create-an-ingress)) — and clients see your virtual cluster bootstrap, never the external endpoints.

What changes compared to the in-cluster backends:

- **Trust material** — trust the *external* cluster's CA (a Secret in `kafka-proxy`), not the Strimzi cluster CA.
- **Egress** — the DMZ netpols are ingress-only, so the proxy can already reach out. If you ever add default-deny **egress** to `kafka-proxy`, open the external bootstrap/broker ports explicitly.
- **Broker-ID ranges** — `nodeIdRanges` must reflect the external cluster's actual node IDs so the proxy's broker-address translation stays consistent.
- **SASL is passthrough — the big one.** The proxy forwards the SASL exchange verbatim; it neither terminates nor translates authentication. The mechanism clients use on the virtual cluster must be the one the *external* listener accepts: for an external OAUTHBEARER listener, clients need tokens from **that cluster's IdP** (its issuer/JWKS — not the `kafka-auth` realm; the [6.10](#610--apply-and-verify) `KAFKA_OPTS` token-endpoint allowlist gotcha applies to those clients too). SCRAM/PLAIN/mTLS backends mean clients present those credentials. There is no SASL translation across mechanisms.
- **Feature compatibility** — check the external cluster's Kafka version/API support against the proxy release; protocol features can surface differently through the proxy.

Why this is one of the proxy's best use cases: an external backend is a cluster **you don't fully control**, and the DMZ pattern gives you a control point at the exit door —

- clients see only your virtual cluster bootstrap: the external cluster's endpoints, TLS material, and broker topology stay in `kafka-proxy`; its own firewall/IP-allowlist only ever admits the proxy namespace, not every client
- the filters from [6.6](#66--optional-add-a-filter) become boundary enforcement: **Record Validation** before data leaves for the external cluster, **Record Encryption** so payloads are only decryptable inside your trust boundary, deny-by-default ACLs ([6.6](#66--optional-add-a-filter), the ACL subsection) for what may be accessed at all

One caveat: the proxy is in the data path, so every produce/fetch makes two hops from your cluster's perspective. If the goal is purely *replicating* data to or from an external cluster (not serving clients), the MirrorMaker 2 plane ([Step 4](04-mirrormaker2.md)) is the better tool — the proxy shines when clients need direct, policy-enforced access.

**Forwarding external topics into an internal cluster** combines the two blocks:

1. **External side** — build the external virtual cluster (the `KafkaService` above + a `VirtualKafkaCluster`) if the external connection should flow through your policy boundary, or let MM2 reach the external cluster directly.
2. **Replication** — add an MM2 mirror ([Step 4](04-mirrormaker2.md)) with `source.bootstrapServers` at the external cluster (directly, or at the external virtual cluster's bootstrap — the proxy fronting the MM2 connection puts your DMZ, ACLs, and filters in the replication path) and `target.alias`/bootstrap at the internal cluster.
3. **Selective replication** — apply the allowlist patterns from [Step 4](04-mirrormaker2.md#41--create-the-namespace-and-the-mm2-resource) (`topicsPattern`/`groupsPattern`) so only the external topics you actually need land internally; the mirrored copies are prefixed with the source alias.
4. **Caveats carry over** — the [SASL-passthrough](#611--optional-proxying-an-external-kafka-cluster) rule (the external listener's mechanism is what MM2 must present), the [platform-policy and support-triangle note](#611--optional-proxying-an-external-kafka-cluster) for the external side, and the NetworkPolicy planning for MM2's egress to the external bootstrap.

**Support posture for external backends:** compatibility with "all types of Kafka instances, irrespective of their distribution" is a *functional* statement, not an SLA. Red Hat supports the proxy's protocol-level connection to a spec-compliant Apache Kafka cluster; issues originating **in the third-party service** belong to that vendor, and no external service (MSK, Confluent Cloud, …) is certified — validate your backend in your environment. Note also that the proxy itself is **Technology Preview in Streams 3.2** (no production SLA at all yet). See the family-wide note in the [README](../README.md#components-and-tested-versions).

**Platform policies and the support triangle.** From the proxy's egress connection onward, the external platform's policies apply — its access policy (accepted auth mechanisms, TLS requirements), its data-governance terms, and its **support policy**, whose supported-client envelope the proxy must stay inside. Two practical consequences: **quota concentration** — all client traffic now originates from one namespace, so the platform's per-connection limits and quotas bind your entire fleet — and the **support triangle** — Red Hat supports the proxy side, the platform supports their service behind a supported client, and *neither* policy covers the integration. Keep the proxy version within the platform's supported client/protocol-version matrix and maintain a **direct-connection baseline** (a test path that bypasses the proxy): when something breaks, it yields the bisect evidence both support organizations will ask for ("fails direct" → their case; "fails only via proxy" → Red Hat's, with a reference-cluster repro).
