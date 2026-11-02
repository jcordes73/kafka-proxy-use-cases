# Demo 2: Record Validation end-to-end

> [↑ Demo scenarios](../10-demos.md) · prev: [Demo 1: Token revocation](01-token-revocation.md) · next: [Demo 3: Live migration / cutover](03-live-migration.md)

> **Deployment order:** the filter deploys **after the proxy** (it wires `filterRefs` into the proxy's virtual cluster); with the demo scenarios at Step 10 that prerequisite is always satisfied. The schema (`APICURIO_CONTENT_ID`) comes from [Step 5.4](../05-apicurio-registry.md#54--register-a-schema).

[`deploy/demos/02-record-validation-demo.yaml`](../../deploy/demos/02-record-validation-demo.yaml) declares the `KafkaProtocolFilter` — a `RecordValidation` rule scoped to `topic-A` that checks record **values** against the registered schema (`apicurioId` = the contentId from Step 5.4). topic-A is **not declared** in the manifest: it already exists on both clusters (created directly with `kafka-topics.sh` in [Step 3.2](../03-kafka-cluster.md#32--create-the-kafka-cluster)). `allowNulls`/`allowEmpty` sit **inside** the rule definition (as siblings of `schemaValidationConfig`), and there is no `defaultRule` — all other topics pass through unvalidated:

```bash
export APICURIO_CONTENT_ID=1
envsubst < deploy/demos/02-record-validation-demo.yaml | oc apply -f -
# deploy/proxy/05-virtualkafkacluster.yaml already wires filterRefs
# [record-validation] on kafka-cluster-1 — the patch below is idempotent:
oc -n kafka-proxy patch virtualkafkacluster kafka-cluster-1 --type=merge \
  -p '{"spec":{"filterRefs":[{"name":"record-validation"}]}}'
oc -n kafka-proxy rollout status deploy/simple --timeout=180s   # proxy reconfigures
```

> ⚠️ **Config shape matters — a malformed filter crash-loops the proxy.** The deployed proxy rejects unknown filter config fields at startup (the pod enters `CrashLoopBackOff` until the `KafkaProtocolFilter` is corrected). Two gotchas verified live: `allowNulls`/`allowEmpty` belong inside the rule definition, and the `wireFormatVersion` option shown in the Red Hat 3.2 guide does not exist in the deployed proxy — omit it.

Produce through the proxy with `kcat` — a conforming record is delivered, a non-conforming one is rejected before the broker:

```bash
echo '{"orderId":"A-1","amount":10}' | oc run kcat-valid --rm -i --restart=Never \
  --image=confluentinc/cp-kcat:latest -- \
  -b kafka-cluster-1-cluster-ip-1-bootstrap.kafka-proxy.svc.cluster.local:9292 \
  -t topic-A -P -l
# (delivered — exit 0)

echo 'this is not json' | oc run kcat-invalid --rm -i --restart=Never \
  --image=confluentinc/cp-kcat:latest -- \
  -b kafka-cluster-1-cluster-ip-1-bootstrap.kafka-proxy.svc.cluster.local:9292 \
  -t topic-A -P -l
# % Delivery failed for message: Broker: Broker failed to validate record
```

Watch `kroxylicious_client_to_proxy_request_total` vs `kroxylicious_proxy_to_server_request_total` in user-workload monitoring — the rejected produce never reaches the upstream (downstream count permanently ahead).

> **To disable validation:** remove the `filterRefs` entry (`oc -n kafka-proxy patch virtualkafkacluster kafka-cluster-1 --type=json --path=/spec/filterRefs --value=[]` and re-apply [`deploy/proxy/05-virtualkafkacluster.yaml`](../../deploy/proxy/05-virtualkafkacluster.yaml) to restore the manifest state), and optionally delete the `KafkaProtocolFilter`. Note the proxy exports no Record Validation–specific metrics — rejections are only visible client-side and in the proxy logs.
