# Step 7: Deploy the Kafka HTTP Bridge (DMZ Client)

> [↑ Setup overview](../openshift-setup.md) · prev: [Step 6: Proxy](06-proxy.md) · next: [Step 8: Console](08-console.md)

The **HTTP Bridge** exposes the Kafka cluster over a REST API (`/topics`, `/consumers`), so HTTP clients — scripts, lightweight services, systems where a Kafka client library is impractical — can produce and consume without a native Kafka protocol connection. Deployed via the Cluster Operator as a `KafkaBridge` resource, it integrates with the proxy by being a **plain Kafka client of the virtual cluster**: point its `bootstrapServers` at the proxy's clusterIP bootstrap Service so HTTP traffic flows **through the proxy**, keeping the DMZ intact.

Placing the bridge in its own **`kafka-http-bridge` namespace** keeps the HTTP API isolated from both the proxy and the brokers — external HTTP clients reach only this namespace, and the bridge (DMZ) reaches the brokers (secure backend) only through the proxy (also in the DMZ). Based on the [Using the Streams for Apache Kafka HTTP Bridge](https://docs.redhat.com/en/documentation/red_hat_streams_for_apache_kafka/3.2/html/using_the_streams_for_apache_kafka_http_bridge) guide.

> **Deployment order:** the bridge is deployed **seventh** in this guide — immediately after the proxy (Step 6), which it connects through (its `bootstrapServers` is the proxy's clusterIP bootstrap, so it can't reach Kafka before the proxy is up); see the Guide map in the [setup overview](../openshift-setup.md).

## 7.1 — Create the namespace and extend the Cluster Operator watch

Create the namespace — the bridge sits in the **DMZ** alongside the proxy, as the
second client-facing door (the brokers stay in the secure backend):

```bash
oc new-project kafka-http-bridge
```

The Cluster Operator ([Step 3](03-kafka-cluster.md#step-3-deploy-the-kafka-cluster-streams-for-apache-kafka)) watches only the `kafka` namespace, so it would not reconcile a `KafkaBridge` in `kafka-http-bridge`. Extend its watch scope:

```bash
# 1. Add kafka-http-bridge to STRIMZI_NAMESPACE in the operator Deployment
oc set env deployment/strimzi-cluster-operator -n kafka STRIMZI_NAMESPACE=kafka,kafka-http-bridge

# 2. Install the RoleBindings for the watched namespace
sed -i 's/namespace: .*/namespace: kafka/' install/cluster-operator/*RoleBinding*.yaml
oc apply -f install/cluster-operator/020-RoleBinding-strimzi-cluster-operator.yaml -n kafka-http-bridge
oc apply -f install/cluster-operator/023-RoleBinding-strimzi-cluster-operator.yaml -n kafka-http-bridge
oc apply -f install/cluster-operator/031-RoleBinding-strimzi-cluster-operator-entity-operator-delegation.yaml -n kafka-http-bridge

# 3. Restart the operator to pick up the new watch scope
oc rollout restart deployment/strimzi-cluster-operator -n kafka
```

 > Alternatively, set `STRIMZI_NAMESPACE="*"` to watch all namespaces (see [Deploying the Cluster Operator to watch all namespaces](https://strimzi.io/docs/operators/latest/deploying.html#deploying-cluster-operator-to-watch-whole-cluster-str)).

> **OLM-installed operator?** Skip the steps above — do not edit the OLM-managed Deployment (OLM reverts it). Instead add the namespace to the operator's `OperatorGroup` and let OLM regenerate the RBAC and roll the operator — either by patching, or (declarative) by applying the updated OperatorGroup manifest:
>
> ```bash
> oc patch operatorgroup <operator-group-name> -n kafka --type merge \
>   -p '{"spec":{"targetNamespaces":["kafka","kafka-http-bridge"]}}'
> # or: oc apply -f deploy/kafka/00-cluster-operator-olm.yaml
> ```

Verify the watch scope before deploying the bridge — if the operator does not
watch `kafka-http-bridge`, the `KafkaBridge` in Step 7.2 is never reconciled
and its Ready wait times out:

```bash
oc get operatorgroup <operator-group-name> -n kafka -o jsonpath='{.spec.targetNamespaces}'
# ["kafka","kafka-http-bridge"]
```

## 7.2 — Deploy the bridge with a `KafkaBridge` resource

The bridge runs in the `kafka-http-bridge` namespace and connects to the **proxy's clusterIP bootstrap** (from [Step 6.10](06-proxy.md#610--apply-and-verify) — verify the exact port with `oc get services -n kafka-proxy`). The ready-to-apply manifest is [`deploy/bridge/01-kafka-bridge.yaml`](../deploy/bridge/01-kafka-bridge.yaml):

```bash
oc apply -f deploy/bridge/01-kafka-bridge.yaml

oc wait --for=condition=Ready kafkabridge/my-bridge -n kafka-http-bridge --timeout=300s
oc get pods -n kafka-http-bridge
```

Field notes (full manifest in the deploy file):

- `spec.bootstrapServers` — the **PROXY's clusterIP bootstrap**, not the Kafka cluster's.
- `spec.tls.trustedCertificates` — the `proxy-ca` Secret (the CA extracted in [Step 6.10, TLS variant](06-proxy.md#610--apply-and-verify), created in THIS namespace, not `kafka-proxy`), so the bridge trusts the proxy's ingress certificate.
- `spec.authentication` — SASL/OAUTHBEARER against the proxied `tls` listener (Step 3.2): `type: custom` with the Strimzi OAuth client handler, `OAUTH_CLIENT_ID`/`OAUTH_CLIENT_SECRET` via the container template from the `kafka-bridge` Keycloak client. Create the Secret first (the realm import does not create Kubernetes Secrets):
  `oc create secret generic kafka-bridge-oauth --from-literal=client-secret=kafka-bridge-secret -n kafka-http-bridge`
- `spec.http.port` — the REST API port (8080).

Expected output:

```text
NAME                          READY  STATUS   RESTARTS
my-bridge-bridge-<pod_id>     1/1    Running  0
```

The operator creates a Service named `<name>-bridge-service` on the configured `http.port`:

```bash
oc get services my-bridge-bridge-service -n kafka-http-bridge
```

## 7.3 — Verify produce/consume over HTTP through the proxy

```bash
# Produce a JSON record through the bridge (and therefore the proxy)
oc run bridge-producer --rm -i --restart=Never --image=curlimages/curl -n kafka-http-bridge -- \
  -X POST http://my-bridge-bridge-service:8080/topics/test-topic \
  -H "Content-Type: application/vnd.kafka.json.v2+json" \
  -d '{"records":[{"value":"hello via bridge + proxy"}]}'
```

```bash
# Create a consumer, subscribe, and fetch records
oc run bridge-consumer --rm -i --restart=Never --image=curlimages/curl -n kafka-http-bridge -- \
  -X POST http://my-bridge-bridge-service:8080/consumers/bridge-consumer-group \
  -H "Content-Type: application/vnd.kafka.v2+json" \
  -d '{"name":"bridge-consumer","format":"json","auto.offset.reset":"earliest"}'

oc run bridge-consumer --rm -i --restart=Never --image=curlimages/curl -n kafka-http-bridge -- \
  -X POST http://my-bridge-bridge-service:8080/consumers/bridge-consumer-group/instances/bridge-consumer/subscription \
  -H "Content-Type: application/vnd.kafka.v2+json" \
  -d '{"topics":["test-topic"]}'

oc run bridge-consumer --rm -i --restart=Never --image=curlimages/curl -n kafka-http-bridge -- \
  -H "Accept: application/vnd.kafka.json.v2+json" \
  http://my-bridge-bridge-service:8080/consumers/bridge-consumer-group/instances/bridge-consumer/records
```

## 7.4 — Operational notes

- **Replicas and HA:** producers are stateless, so multiple bridge replicas work well for producing workloads behind a load balancer. **Consumers are NOT recommended with multiple replicas** — consumer state (subscriptions, offsets in progress) is held in-memory per instance with no failover; if the pod dies, HTTP clients must recreate the consumer. Plan consumer-facing traffic accordingly.
- **HTTP interface security is not built in:** the bridge's REST API has no authentication/authorization of its own. Restrict access with NetworkPolicies (see [12.5](#75--restrict-http-access-to-the-bridge-dmz-networkpolicy)), or front the bridge with a reverse proxy (e.g., OAuth 2.0) or API gateway.
- **Bridge → Kafka auth:** the `KafkaBridge` CR supports `spec.authentication` (`tls`, `scram-sha-256`, `scram-sha-512`, `plain`, `custom`) and `spec.tls.trustedCertificates` — the bridge authenticates to the *Kafka listener* through the proxy like any other client. The proxied `tls` listener requires SASL/OAUTHBEARER (Step 3.2), so the bridge uses `custom` authentication with the Strimzi OAuth client handler, obtaining tokens from the `kafka-bridge` Keycloak client ([Step 2.5](02-keycloak.md#25--client-authentication-at-the-proxy)); the bridge's HTTP API itself remains guarded by the namespace DMZ.
- **Exposing the bridge outside the cluster:** use an OpenShift Route for the `my-bridge-bridge-service` (HTTP clients are exempt from the Kafka-protocol constraints that make Route exposure problematic for the proxy itself).
- **DMZ consistency:** with the bridge in `kafka-http-bridge`, the same trust boundary applies — the bridge is just another client of the proxy, and the Kafka namespace still only accepts connections from `kafka-proxy`. Add the NetworkPolicy from the next section to enforce which clients can reach the bridge's HTTP port.

## 7.5 — Restrict HTTP access to the bridge (DMZ NetworkPolicy)

The bridge's REST API has no authentication of its own (see the note above), so enforce access at the network layer. The bridge pods carry the Strimzi labels `strimzi.io/kind: KafkaBridge` and `strimzi.io/cluster: my-bridge`. Combine two additive policies in the `kafka-http-bridge` namespace:

1. **Default deny** — block all ingress to the bridge pods.
2. **Allowed clients** — permit ingress on the HTTP port 8080 from within the namespace (the `oc` test pods in 12.3) and from the namespace running your API layer / gateway (example: `api-gateway` — change to yours).

The ready-to-apply manifest is [`deploy/bridge/02-bridge-netpol-dmz.yaml`](../deploy/bridge/02-bridge-netpol-dmz.yaml) — it also ships the stand-in `api-gateway` namespace for the verification below:

```bash
oc apply -f deploy/bridge/02-bridge-netpol-dmz.yaml
oc get networkpolicies -n kafka-http-bridge
```

Verify the lockdown behaves as intended:

```bash
# From the api-gateway namespace: connection succeeds (replace with your API layer's namespace;
# the stand-in api-gateway namespace ships with the DMZ NetworkPolicy manifest above)
oc run bridge-allowed --rm -i --restart=Never --image=curlimages/curl -n api-gateway -- \
  --max-time 5 -s -o /dev/null -w "%{http_code}\n" \
  http://my-bridge-bridge-service.kafka-http-bridge.svc.cluster.local:8080/health

# From an unrelated namespace (e.g., default): connection should TIME OUT
oc run bridge-blocked --rm -i --restart=Never --image=curlimages/curl -n default -- \
  --max-time 5 -s -o /dev/null -w "%{http_code}\n" \
  http://my-bridge-bridge-service.kafka-http-bridge.svc.cluster.local:8080/health
```

Notes:

- **Egress is untouched:** these policies restrict ingress only, so the bridge can still connect to the proxy's bootstrap (`kafka-proxy:9292`). If you later add a default-deny **egress** policy to `kafka-http-bridge`, remember to allow egress to `kafka-proxy` on the proxy's bootstrap port.
- **Cluster Operator:** the operator reconciles the bridge via the Kubernetes API and pod-local probes — it does not need network access to port 8080, so the default-deny does not affect reconciliation.
- **Expose the bridge through your API layer:** front the `my-bridge-bridge-service` with your gateway (in the allowed namespace) rather than exposing the bridge directly — the gateway can add the authentication/authorization the HTTP interface lacks.
