# Demo 6: Authorization at the proxy

> [↑ Demo scenarios](../11-demos.md) · prev: [Demo 5: Stable endpoints](05-stable-endpoints.md) · next: [Demo 7: Entity isolation](07-entity-isolation.md)

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

3. The allow path: demo-client produces to `topic-A` — works.
4. The deny path: `camel-producer` (or the demo-client on any other topic) is rejected with `TOPIC_AUTHORIZATION_FAILED` **before the brokers** — the request never leaves the proxy.
5. Revert: `oc apply -f deploy/proxy/05-virtualkafkacluster.yaml`

> The Authorization filter adds checks *in addition to* the broker's own; both must allow. The brokers currently have no authorization configured, so the proxy's decision is the effective one.
