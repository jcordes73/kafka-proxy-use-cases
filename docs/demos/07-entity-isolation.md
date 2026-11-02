# Demo 7: Entity isolation (preview)

> [↑ Demo scenarios](../11-demos.md) · prev: [Demo 6: Authorization at the proxy](06-proxy-authorization.md) · next: [Demo 8: Token validation at the proxy](08-token-validation.md)

Each authenticated principal gets a **private namespace for consumer groups and transactional IDs** within the shared cluster — no coordination needed, no collisions possible.

1. Apply the demo manifest (SaslInspection + EntityIsolation):

   ```bash
   oc apply -f deploy/demos/07-entity-isolation.yaml
   ```

2. Run **two** consumers through the proxy using the *same* group name (e.g. `demo-consumer-group`) but different OAuth clients (`demo-client` and `camel-producer` tokens).
3. On the brokers, the groups materialize as `<principal>-demo-consumer-group` — two distinct groups. List them from inside the cluster (bypassing the proxy) to show both exist.
4. Neither consumer sees or interferes with the other's offsets. `GROUP_ID` and `TRANSACTIONAL_ID` are isolated; topic names are not yet ([kroxylicious#3504](https://github.com/kroxylicious/kroxylicious/issues/3504)).
5. Revert: `oc apply -f deploy/proxy/05-virtualkafkacluster.yaml`
