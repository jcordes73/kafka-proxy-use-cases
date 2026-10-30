# Step 6: Deploy the Streams for Apache Kafka Proxy (kafka-proxy namespace)

> [↑ Setup overview](../openshift-setup.md) · prev: [Step 5: Apicurio Registry](05-apicurio-registry.md) · next: [Step 7: HTTP Bridge](07-http-bridge.md)

This step covers the whole proxy deployment in subsections 6.1–6.10: install the
operator, understand the custom resources, create the proxy / ingress / backend
cluster / virtual cluster, wire TLS and DNS, and verify the virtual cluster end to end.

> **Placement:** the proxy runs in the **DMZ** (`kafka-proxy`) — it is the only door into the
> Kafka clusters, which sit in the **secure backend** (`kafka-cluster-1`/`kafka-cluster-2`) and
> accept Kafka-protocol connections from the proxy namespace only ([Step 3.4](03-kafka-cluster.md#34--restrict-kafka-ingress-to-the-proxy-namespace-dmz)).

## 6.1 — Install the Proxy Operator

### Recommended: upstream release artifacts (as deployed here)

The deployed operator is the **upstream kroxylicious 0.25.1 release** (see the divergences box at the top of this guide). [`deploy/proxy/01-proxy-operator-install.yaml`](../deploy/proxy/01-proxy-operator-install.yaml) is derived from the release's `kroxylicious-operator-0.25.1-install.yaml` with the namespace and Deployment renamed `kroxylicious-operator` → `kafka-proxy` — apply it as is:

```bash
oc apply -f deploy/proxy/01-proxy-operator-install.yaml

oc wait --for=condition=Available deployment/kafka-proxy \
  -n kafka-proxy --timeout=120s

oc get deployments -n kafka-proxy
```

Expected output:

```text
NAME                      READY  UP-TO-DATE  AVAILABLE
kafka-proxy               1/1    1           1
```

### Alternative: Red Hat subscription artifacts

Download the release archive `streams-3.2-ocp-install-examples.zip` from the [Streams for Apache Kafka software downloads page](https://access.redhat.com/jbossnetwork/restricted/listSoftware.html?downloadType=distributions&product=jboss.amq.streams) and extract it — `install/` contains the YAML manifests for the operator, `examples/` example custom resources for deploying a proxy:

```bash
unzip streams-3.2-ocp-install-examples.zip -d streams-kafka-proxy
cd streams-kafka-proxy

oc new-project kafka-proxy

# Linux (macOS: sed -i '')
sed -i 's/namespace: .*/namespace: kafka-proxy/' install/*.yaml

oc create -f install
```

> The Red Hat build names the operator Deployment `kroxylicious-operator` (as does the upstream release file before this guide's rename) — keep that name in the steps that follow, or rename it to `kafka-proxy` to match this guide.

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

The `KafkaService` describes the upstream Kafka cluster. Declare the full range of broker node IDs. The ready-to-apply manifest is [`deploy/proxy/04-kafkaservice.yaml`](../deploy/proxy/04-kafkaservice.yaml) — one `KafkaService` per backend cluster (`kafka-cluster-1` and `kafka-cluster-2`), each using `strimziKafkaRef` to discover the bootstrap address from the Strimzi `Kafka` CR's status and `trustStrimziCaCertificate: true` to auto-trust the cluster CA (no hardcoded bootstrap, no copied CA):

```bash
oc apply -f deploy/proxy/04-kafkaservice.yaml
```

Field notes (full manifests in the deploy file):

- `spec.strimziKafkaRef` — `name`/`namespace` of the Strimzi `Kafka` resource and the `listenerName: tls` to connect over (requires Strimzi 0.49+; specify either `strimziKafkaRef` or `bootstrapServers`, not both).
- `spec.tls.trustAnchorRef` — for non-Strimzi clusters; accepts a `Secret`, and the operator can auto-trust the Strimzi-generated CA anchor for Streams-managed clusters.
- `nodeIdRanges` must cover **all** broker node IDs in the target cluster (`start` and `end` inclusive) — `0`–`2` for the cluster deployed in [Step 3](03-kafka-cluster.md#step-3-deploy-the-kafka-cluster-streams-for-apache-kafka). For an external cluster, use `bootstrapServers` with its reachable address instead.

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

## 6.7 — Create the VirtualKafkaCluster

The `VirtualKafkaCluster` wires everything together. All referenced resources must be in the same namespace. Filter order matters: client requests pass through filters from index `0` to `n`; broker responses pass through in reverse order. An empty/omitted `filterRefs` passes traffic through unchanged.

The ready-to-apply manifest is [`deploy/proxy/05-virtualkafkacluster.yaml`](../deploy/proxy/05-virtualkafkacluster.yaml) — **one virtual cluster per backend cluster** (`kafka-cluster-1` and `kafka-cluster-2`), each wiring `proxyRef: simple`, its `targetKafkaServiceRef`, its own `clusterIP` ingress (`cluster-ip-1` / `cluster-ip-2` with a shared TLS `certificateRef: proxy-tls`, applied in [Step 6.8](#68--tls-certificates)), and `filterRefs` — `[record-validation]` on `kafka-cluster-1` (created in [Step 5](05-apicurio-registry.md#step-5-deploy-apicurio-registry-and-validate-records-record-validation); if you are running strictly in order, remove that ref until Step 5) and none on `kafka-cluster-2` (traffic passes through unchanged):

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

3. **Wait for the proxy pods to be ready.** The operator creates a `Deployment` named after the `KafkaProxy` resource (here: `simple`):

   ```bash
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
         --topic test-topic --command-config /tmp/client.properties
       bin/kafka-console-consumer.sh \
         --bootstrap-server kafka-cluster-1-cluster-ip-1-bootstrap.kafka-proxy.svc.cluster.local:9292 \
         --topic test-topic --from-beginning --timeout-ms 10000 \
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
         --topic test-topic --command-config /tmp/client.properties
     '
   ```

   Notes for the TLS variant:

   - Connect using the **SAN name from the server certificate** (the generated bootstrap hostname) so SNI and certificate validation succeed — never an IP address.
   - The proxy relays the client's SASL/OAUTHBEARER exchange to the brokers (SASL passthrough) — the broker validates the JWT against Keycloak's JWKS, so the token must come from a client in the `kafka` realm.
   - Kafka 4.x rejects **http** token endpoints unless allowlisted — the `KAFKA_OPTS` env var above sets `org.apache.kafka.sasl.oauthbearer.allowed.urls`. Using the Keycloak **https** route instead removes the need for it (but the pod must then trust Keycloak's CA).
   - `openShiftRoute` bootstrap endpoints use port **443**.
   - If the ingress uses client-certificate (mutual TLS) authentication, add `ssl.keystore.location`, `ssl.keystore.password`, and `ssl.key.password` as required.

7. Continue with [Getting Started](../README.md#getting-started-on-openshift) to add filters.
