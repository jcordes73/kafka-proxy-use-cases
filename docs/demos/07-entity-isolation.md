# Demo 7: Entity isolation (preview)

> [↑ Demo scenarios](../10-demos.md) · prev: [Demo 6: Authorization at the proxy](06-proxy-authorization.md) · next: [Demo 8: Token validation on the Kafka cluster](08-token-validation.md)

Each authenticated principal gets a **private namespace for consumer groups and transactional IDs** within the shared cluster — no coordination needed, no collisions possible.

> ✅ **Verified live** (this is the only way the demo works): with two principals using the *same* client-visible group name, the brokers hold two **distinct** groups — `<sub-UUID>_<group>` — with fully independent offsets. Note the mapped name embeds the JWT `sub` UUID, not `service-account-<clientId>`: `SaslInspection` derives the principal from the JWT `sub` claim (a UUID), so the cluster-visible group name is `<sub-UUID>_<client-visible-name>` (e.g. `7f83e84f-12c3-4097-b638-62f1af4a7a78_demo-consumer-group` and `0a59b3ac-c431-4d46-b037-456b97e4f06f_demo-consumer-group` — same visible name, two different groups, each with its own committed offsets).

1. Apply the demo manifest (SaslInspection + EntityIsolation):

   ```bash
   oc apply -f deploy/demos/07-entity-isolation.yaml
   ```

   ⚠️ **The `separator` matters — verified live:** the SaslInspection-derived principal is the JWT `sub` **UUID**, which contains `-`. With `separator: "-"` every request fails with `FIND_COORDINATOR failed: Principal '...' is unacceptable as it contains the mapping separator '-'`; with `~` the proxy rejects the filter **at startup** (`'~' is an unacceptable separator` — the mapper validates the character, and a rejected filter crash-loops the proxy). Use `_` — it passes validation and cannot appear in a UUID.

2. Run **two** consumers through the proxy using the *same* group name (e.g. `demo-consumer-group`) but different OAuth clients (`demo-client` and `camel-producer` tokens). Use `--max-messages 1` so the consumers exit promptly — the live `camel-producer` Deployment feeds `topic-A` continuously, so `--timeout-ms` alone never fires.

3. On the brokers, the groups materialize as `<sub-UUID>_demo-consumer-group` — two distinct groups. List them from inside the cluster (bypassing the proxy) to show both exist — verified output:

   ```text
   $ bin/kafka-consumer-groups.sh --bootstrap-server kafka-cluster-1-kafka-bootstrap...:9093 --list ...
   7f83e84f-12c3-4097-b638-62f1af4a7a78_demo-consumer-group
   0a59b3ac-c431-4d46-b037-456b97e4f06f_demo-consumer-group
   ```

4. Neither consumer sees or interferes with the other's offsets — verified live: the two groups hold **independent** offsets on `topic-A` (demo-client's group at `1/1/252`, camel-producer's at `0/1/0` across partitions 0–2). `GROUP_ID` and `TRANSACTIONAL_ID` are isolated; topic names are not yet.
5. Revert: `oc apply -f deploy/proxy/05-virtualkafkacluster.yaml`

> ⚠️ **While the demo wiring is active**, other proxy clients (e.g. the live `camel-consumer` Deployment) are rejected with the same mapping error and reconnect — they recover on revert. This is expected: the demo replaces the virtual cluster's filter wiring.
