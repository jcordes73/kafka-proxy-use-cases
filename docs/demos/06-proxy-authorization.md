# Demo 6: Authorization at the proxy

> [↑ Demo scenarios](../10-demos.md) · prev: [Demo 5: Stable endpoints](05-stable-endpoints.md) · next: [Demo 7: Entity isolation](07-entity-isolation.md)

Enforce per-principal ACLs **before requests reach the brokers**. Requires the authenticated subject: `SaslInspection` extracts the end-client principal from the SASL/OAUTHBEARER exchange.

1. Discover the principal SaslInspection derives for the demo-client — the default subject builder uses the **SASL authorized ID**, which for OAUTHBEARER is the token's `sub` claim (a UUID for Keycloak service accounts):

   ```bash
   echo ${ACCESS_TOKEN} | jq -R 'split(".") | .[1] | @base64d | fromjson | .sub'
   ```

2. Put it in the ACL rules — replace `<DEMO-CLIENT-SUBJECT>` in [`deploy/demos/06-authorization-filters.yaml`](../../deploy/demos/06-authorization-filters.yaml) (also grants `demo-*` groups; `otherwise deny` denies everything else), then:

   ```bash
   oc apply -f deploy/demos/06-authorization-filters.yaml
   oc wait --for=condition=Accepted=True virtualkafkacluster/kafka-cluster-1 -n kafka-proxy --timeout=120s
   ```

   > ✅ **Verified live:** with the demo-client subject `7f83e84f-12c3-4097-b638-62f1af4a7a78` (the JWT `sub` UUID — matching the Entity Isolation demo, the SaslInspection-derived principal is the `sub` claim, not `service-account-<clientId>`), the virtual cluster goes `Accepted=True` and the proxy pod rolls.

3. The allow path: demo-client produces a schema-valid record to `topic-A` — works. Verified output (produce, then consume back through the proxy with `--group demo-consumer` to prove the record reached the broker):

   ```text
   === ALLOW: demo-client produces to topic-A ===
   PRODUCE OK (request accepted)

   $ kafka-console-consumer.sh --topic topic-A --from-beginning --max-messages 200 --group demo-consumer ...
   {"orderId":"ORD-1","amount":42.5}      # ← the allowed demo-client record, on the broker
   ```

   ⚠️ **The consumer group must match the ACL too:** the console consumer's default group is a random `console-consumer-XXXXX`, which the rules deny — use `--group demo-consumer` (the `demo-*` group grant) or the request fails with `GroupAuthorizationException: Not authorized to access group: console-consumer-69340`.

4. The deny paths: `camel-producer` (or the demo-client on any other topic) is rejected with `TOPIC_AUTHORIZATION_FAILED` **before the brokers** — the request never leaves the proxy. Verified outputs:

   ```text
   === DENY 1: camel-producer produces to topic-A (intentionally ungranted) ===
   TopicAuthorizationException: Not authorized to access topics: [topic-A]

   === DENY 2: demo-client produces to topic-B (granted topic-A only) ===
   TopicAuthorizationException: Not authorized to access topics: [topic-B]
   ```

   > ⚠️ **Gotcha — the allow path needs a schema-valid record.** The `record-validation` filter sits in front of the Authorization filter on `kafka-cluster-1`, so an invalid record is rejected with `InvalidRecordException` before ACLs are even checked. And if the schema registered in the registry under the filter's `apicurioId` has drifted from what the demo produces (e.g. after a sandbox restore), every allow-path produce fails validation. Check the schema the filter pins: `oc -n kafka-proxy get kafkaprotocolfilter record-validation -o jsonpath='{.spec.configTemplate.rules[0].valueRule.schemaValidationConfig.apicurioId}'`, then fetch that `contentId` from the registry and re-register (or re-point the filter) if it drifted.

5. Revert: `oc apply -f deploy/proxy/05-virtualkafkacluster.yaml` (restores the baseline `filterRefs`; the demo's `acl-rules` ConfigMap and `proxy-authorization` filter can then be deleted).

> The Authorization filter adds checks *in addition to* the broker's own; both must allow. The brokers currently have no authorization configured, so the proxy's decision is the effective one.
