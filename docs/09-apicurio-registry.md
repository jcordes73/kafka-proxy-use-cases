# Step 17: Deploy Apicurio Registry and Validate Records (Record Validation)

> [↑ Setup overview](../openshift-setup.md) · prev: [Step 16: Custom filter](08-custom-filter.md) · next: [Step 18: MirrorMaker 2](10-mirrormaker2.md)

The **Record Validation filter** (Technology Preview in 3.2 — see the Red Hat [Record Validation filter guide](https://docs.redhat.com/en/documentation/red_hat_streams_for_apache_kafka/3.2/html/record_validation_filter_guide)) blocks poison messages at the proxy: records that fail validation are rejected and never reach the brokers. Its **schema validation** mode requires the **Red Hat build of Apicurio Registry 3.x** — 2.x support was removed in 3.2, and 3.x identifies schemas by `contentId` (the filter's `apicurioId`; clients still embedding `globalId` fail validation). Only **JSON schemas** are enforced.

This step deploys the registry in a `kafka-registry` namespace, registers a demo schema, and wires a validation rule scoped to a demo topic — the rest of the data path stays unvalidated (no `defaultRule`). See use case [3 in the guide](../README.md#3-data-quality-blocking-poison-messages-record-validation-filter).

## 17.1 — Install the Apicurio Registry operator

[`deploy/apicurio/01-subscription.yaml`](../deploy/apicurio/01-subscription.yaml) creates the `kafka-registry` namespace, an OwnNamespace `OperatorGroup`, and a `Subscription` for the `apicurio-registry-3` package (Red Hat Operators catalog, channel `3.2.x` — aligned with the Streams 3.2 LTS line), with Manual approval so OLM does not auto-upgrade the registry:

```bash
oc apply -f deploy/apicurio/01-subscription.yaml
oc -n kafka-registry get installplan          # then approve:
oc -n kafka-registry patch installplan <name> --type merge -p '{"spec":{"approved":true}}'
oc -n kafka-registry wait --for=jsonpath='{.status.phase}'=Succeeded csv/apicurio-registry-3.v3.2.6-r2 --timeout=300s
```

## 17.2 — Deploy the registry (PostgreSQL storage)

Apicurio Registry 3 offers `kafkasql`, `kubernetesops`, `mysql`, and `postgresql` storage. `kafkasql` would reuse the Kafka cluster but requires widening the [Step 0.4](01-kafka-cluster.md#04--restrict-kafka-ingress-to-the-proxy-namespace-dmz) DMZ NetworkPolicy to admit the registry namespace — `postgresql` keeps the DMZ closed. [`deploy/apicurio/02-postgres.yaml`](../deploy/apicurio/02-postgres.yaml) is a dev-only ephemeral PostgreSQL (mirrors the Keycloak pattern from [Step 1.2](02-keycloak.md#12--provision-a-database-dev-ephemeral-postgresql) — data is lost on restart); the DB credentials are applied as a Secret (idempotent via `oc apply`) and referenced from the `ApicurioRegistry3` resource:

```bash
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
oc -n kafka-registry wait --for=condition=Ready apicurioregistry3/apicurio-registry --timeout=300s
```

The `ApicurioRegistry3` resource (note the component-based API — storage config lives under `spec.app`): the registry and UI deployments run without Routes (the proxy fetches schemas over the in-cluster HTTP service), the operator-generated NetworkPolicies (allow-all ingress on 8080/9000) are disabled in favor of the Step 17.3 lockdown, and `spec.app.storage.sql.dataSource` carries the JDBC URL, the username as a plain string, and the password as a Secret reference. Verify the storage backend took effect:

```bash
oc -n kafka-registry logs deploy/apicurio-registry-app-deployment | grep 'Using.*SQL storage'
# Using postgresql SQL storage.
```

## 17.3 — Lock down the registry network

[`deploy/apicurio/04-netpol-dmz.yaml`](../deploy/apicurio/04-netpol-dmz.yaml) mirrors the Kafka DMZ pattern ([Step 0.4](01-kafka-cluster.md#04--restrict-kafka-ingress-to-the-proxy-namespace-dmz)): default-deny ingress to the registry pods, same-namespace traffic (registry ↔ PostgreSQL, UI), and ingress from the `kafka-proxy` namespace only on the REST port 8080:

```bash
oc apply -f deploy/apicurio/04-netpol-dmz.yaml
```

Verify the lockdown behaves as intended (mirrors the [Step 0.4](01-kafka-cluster.md#04--restrict-kafka-ingress-to-the-proxy-namespace-dmz) test) — from
another namespace the registry is unreachable, from `kafka-proxy` the REST API
answers:

```bash
# From another namespace (e.g., default): connection times out
oc run registry-blocked --rm -i --restart=Never --image=curlimages/curl -n default -- \
  -s -o /dev/null -w '%{http_code}' --max-time 10 --connect-timeout 8 \
  http://apicurio-registry-app-service.kafka-registry.svc.cluster.local:8080/apis/registry/v3/system/info
# 000 (connection failed)

# From the kafka-proxy namespace: reachable (this is what the filter uses)
oc run registry-allowed --rm -i --restart=Never --image=curlimages/curl -n kafka-proxy -- \
  -s -w '\nHTTP %{http_code}\n' --max-time 10 \
  http://apicurio-registry-app-service.kafka-registry.svc.cluster.local:8080/apis/registry/v3/system/info
# HTTP 200
```

> The operator's `spec.app.networkPolicy.enabled: false` (and the matching `ui` setting) must be set **before** first deploy — on an existing resource the operator keeps re-creating its allow-all policies. If they appear anyway, delete them and re-apply the CR.

## 17.4 — Register a schema

Schema validation enforces a schema identified by its **`contentId`**. Register a JSON schema through the registry's v3 REST API (from a pod in `kafka-registry` — same-namespace traffic is allowed) and capture the `contentId` from the response:

```bash
echo '{
  "artifactId": "record-validation-demo-value",
  "artifactType": "JSON",
  "firstVersion": {
    "content": {
      "content": "{\"$schema\":\"https://json-schema.org/draft/2020-12/schema\",\"type\":\"object\",\"properties\":{\"orderId\":{\"type\":\"string\"},\"amount\":{\"type\":\"number\"}},\"required\":[\"orderId\",\"amount\"],\"additionalProperties\":false}",
      "contentType": "application/json"
    }
  }
}' | oc run -i --rm registry-curl --restart=Never --image=curlimages/curl -n kafka-registry -- \
  -s -X POST -H 'Content-Type: application/json' --data-binary @- \
  http://apicurio-registry-app-service:8080/apis/registry/v3/groups/default/artifacts

# …"version":{"version":"1", … "contentId":1, …}
export APICURIO_CONTENT_ID=1
```

## 17.5 — Demo topic and Record Validation filter

[`deploy/apicurio/05-record-validation-demo.yaml`](../deploy/apicurio/05-record-validation-demo.yaml) declares the demo topic and the `KafkaProtocolFilter` — a `RecordValidation` rule scoped to `record-validation-demo` that checks record **values** against the registered schema (`apicurioId` = the contentId from Step 17.4). `allowNulls`/`allowEmpty` sit **inside** the rule definition (as siblings of `schemaValidationConfig`), and there is no `defaultRule` — all other topics pass through unvalidated:

```bash
envsubst < deploy/apicurio/05-record-validation-demo.yaml | oc apply -f -
# deploy/proxy/05-virtualkafkacluster.yaml already wires filterRefs
# [record-validation] on kafka-cluster-1 — the patch below is idempotent:
oc -n kafka-proxy patch virtualkafkacluster kafka-cluster-1 --type=merge \
  -p '{"spec":{"filterRefs":[{"name":"record-validation"}]}}'
oc -n kafka-proxy rollout status deploy/simple --timeout=180s   # proxy reconfigures
```

> ⚠️ **Config shape matters — a malformed filter crash-loops the proxy.** The deployed upstream 0.25.1 proxy rejects unknown filter config fields at startup (the pod enters `CrashLoopBackOff` until the `KafkaProtocolFilter` is corrected). Two gotchas verified live: `allowNulls`/`allowEmpty` belong inside the rule definition, and the `wireFormatVersion` option shown in the Red Hat 3.2 guide does not exist in 0.25.1 — omit it.

## 17.6 — Verify validation end-to-end

Produce through the proxy with `kcat` — a conforming record is delivered, a non-conforming one is rejected before the broker:

```bash
echo '{"orderId":"A-1","amount":10}' | oc run kcat-valid --rm -i --restart=Never \
  --image=confluentinc/cp-kcat:latest -- \
  -b kafka-cluster-1-cluster-ip-1-bootstrap.kafka-proxy.svc.cluster.local:9292 \
  -t record-validation-demo -P -l
# (delivered — exit 0)

echo 'this is not json' | oc run kcat-invalid --rm -i --restart=Never \
  --image=confluentinc/cp-kcat:latest -- \
  -b kafka-cluster-1-cluster-ip-1-bootstrap.kafka-proxy.svc.cluster.local:9292 \
  -t record-validation-demo -P -l
# % Delivery failed for message: Broker: Broker failed to validate record
```

Confirm the topic holds only the valid records — consume through the HTTP Bridge ([Step 12](04-http-bridge.md#step-12-deploy-the-kafka-http-bridge-dmz-client); create a consumer, subscribe, fetch):

```bash
oc run bridge-consumer --rm -i --restart=Never --image=curlimages/curl -n kafka-http-bridge -- \
  -X POST http://my-bridge-bridge-service:8080/consumers/record-validation-verify \
  -H "Content-Type: application/vnd.kafka.v2+json" \
  -d '{"name":"verify-consumer","format":"json","auto.offset.reset":"earliest"}'

oc run bridge-consumer --rm -i --restart=Never --image=curlimages/curl -n kafka-http-bridge -- \
  -X POST http://my-bridge-bridge-service:8080/consumers/record-validation-verify/instances/verify-consumer/subscription \
  -H "Content-Type: application/vnd.kafka.v2+json" \
  -d '{"topics":["record-validation-demo"]}'

# allow ~20–30 s for the group to join through the proxy, then:
oc run bridge-consumer --rm -i --restart=Never --image=curlimages/curl -n kafka-http-bridge -- \
  -H "Accept: application/vnd.kafka.json.v2+json" \
  http://my-bridge-bridge-service:8080/consumers/record-validation-verify/instances/verify-consumer/records
# [{"topic":"record-validation-demo", … "value":{"orderId":"A-1","amount":10} …}]
```

> **To disable validation:** remove the `filterRefs` entry (`oc -n kafka-proxy patch virtualkafkacluster kafka-cluster-1 --type=json --path=/spec/filterRefs --value=[]` and re-apply [`deploy/proxy/05-virtualkafkacluster.yaml`](../deploy/proxy/05-virtualkafkacluster.yaml) to restore the manifest state), delete the `KafkaProtocolFilter`, and optionally tear down the namespace (`oc delete namespace kafka-registry`). Note the 0.25.1 proxy exports no Record Validation–specific metrics — rejections are only visible client-side and in the proxy logs.
