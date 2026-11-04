# Demo 7: Entity isolation (preview)

> [↑ Demo scenarios](../10-demos.md) · prev: [Demo 6: Authorization at the proxy](06-proxy-authorization.md) · next: [Demo 8: Token validation on the Kafka cluster](08-token-validation.md)

Each authenticated principal gets a **private namespace for consumer groups and transactional IDs** within the shared cluster — no coordination needed, no collisions possible. Like [Demo 5](05-stable-endpoints.md) and [Demo 6](06-proxy-authorization.md), this demo uses the **real realm users** `kafka-user-a` and `kafka-user-b` through a throwaway Kafka CLI pod — two principals that share the *same* client-visible group name and still never collide.

> ✅ **Verified live** (this is the only way the demo works): with two principals using the *same* group name, the brokers hold two **distinct** groups — `<sub-UUID>_<group>` — with fully independent offsets. The mapped name embeds the JWT `sub` UUID, not `service-account-<clientId>`: `SaslInspection` derives the principal from the JWT `sub` claim (a UUID), so the cluster-visible group name is `<sub-UUID>_<client-visible-name>`.

## Run it

**1. Apply the demo manifest** (SaslInspection + EntityIsolation) — it **replaces** the `kafka-cluster-1` `VirtualKafkaCluster` wiring:

```bash
oc apply -f deploy/demos/07-entity-isolation.yaml
oc -n kafka-proxy wait --for=condition=Accepted=True virtualkafkacluster/kafka-cluster-1 --timeout=120s
```

⚠️ **The `separator` matters — verified live:** the SaslInspection-derived principal is the JWT `sub` **UUID**, which contains `-`. With `separator: "-"` every request fails with `FIND_COORDINATOR failed: Principal '...' is unacceptable as it contains the mapping separator '-'`; with `~` the proxy rejects the filter **at startup** (`'~' is an unacceptable separator` — the mapper validates the character, and a rejected filter crash-loops the proxy). Use `_` — it passes validation and cannot appear in a UUID.

**2. Run the Kafka CLI pod** with the proxy CA mounted (as in [Demo 5](05-stable-endpoints.md)):

```bash
oc -n kafka-proxy get secret proxy-tls -o jsonpath='{.data.tls\.crt}' | base64 -d > /tmp/proxy-ca.crt
oc -n kafka-proxy apply -f - <<EOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: proxy-ca
  namespace: kafka-proxy
data:
  ca.crt: |
$(sed 's/^/    /' /tmp/proxy-ca.crt)
EOF
rm -f /tmp/proxy-ca.crt

oc -n kafka-proxy run kafka-cli --restart=Never --image=quay.io/strimzi/kafka:latest-kafka-4.2.2 \
  --overrides='{
    "spec": {
      "securityContext": {"seccompProfile": {"type": "RuntimeDefault"}, "runAsNonRoot": true},
      "containers": [{
        "name": "kafka-cli",
        "image": "quay.io/strimzi/kafka:latest-kafka-4.2.2",
        "command": ["sleep", "1800"],
        "volumeMounts": [{"name": "proxy-ca", "mountPath": "/opt/proxy-ca", "readOnly": true}],
        "securityContext": {"allowPrivilegeEscalation": false, "capabilities": {"drop": ["ALL"]}, "runAsNonRoot": true}
      }],
      "volumes": [{"name": "proxy-ca", "configMap": {"name": "proxy-ca"}}]
    }
  }' --command -- sleep 1800
```

**3. One client config per user** — password grants via the `kafka-console` client (Strimzi's `JaasClientOauthLoginCallbackHandler`; the CLI's native OAuth handler cannot authenticate a password user — see the [Demo 5 gotchas](05-stable-endpoints.md)):

```bash
oc -n kafka-proxy exec kafka-cli -- bash -c '
    mkcfg() { cat > /tmp/cfg-$1.properties <<EOF
security.protocol=SASL_SSL
sasl.mechanism=OAUTHBEARER
sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required oauth.token.endpoint.uri="http://keycloak-http.kafka-auth.svc.cluster.local:8080/realms/kafka/protocol/openid-connect/token" oauth.client.id="kafka-console" oauth.client.secret="kafka-console-secret" oauth.password.grant.username="$2" oauth.password.grant.password="$3";
sasl.login.callback.handler.class=io.strimzi.kafka.oauth.client.JaasClientOauthLoginCallbackHandler
ssl.truststore.type=PEM
ssl.truststore.location=/opt/proxy-ca/ca.crt
EOF
}
    mkcfg a kafka-user-a password-a
    mkcfg b kafka-user-b password-b
'
```

**4. Run two consumers with the SAME group name** (`isolation-demo-group`), one per user. Use `--max-messages 1` so the consumers exit promptly — the live `camel-producer` Deployment feeds `topic-A` continuously, so `--timeout-ms` alone never fires (all verified live — each consumer reads `Processed a total of 1 messages`):

```bash
oc -n kafka-proxy exec kafka-cli -- bash -c '
    B=kafka-cluster-1-cluster-ip-1-bootstrap.kafka-proxy.svc.cluster.local:9292
    bin/kafka-console-consumer.sh --bootstrap-server $B --topic topic-A --from-beginning \
      --max-messages 1 --timeout-ms 20000 --group isolation-demo-group --command-config /tmp/cfg-a.properties
    bin/kafka-console-consumer.sh --bootstrap-server $B --topic topic-A --from-beginning \
      --max-messages 1 --timeout-ms 20000 --group isolation-demo-group --command-config /tmp/cfg-b.properties
'
```

**5. On the brokers, the groups materialize as `<sub-UUID>_isolation-demo-group`** — two distinct groups. List them from inside the cluster (bypassing the proxy, plain listener 9092) to show both exist — verified output:

```text
$ oc -n kafka-cluster-1 exec kafka-cluster-1-kafka-0 -- \
    /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list
7a697d11-9bc3-45a5-a4c0-b741c8a0d540_isolation-demo-group
b7cf8efb-ebaf-42f8-a836-5dd7377a5f9b_isolation-demo-group
```

**6. Neither user sees or interferes with the other's offsets** — verified live, `--describe` shows the two groups holding **independent** committed offsets on `topic-A` (user-a's group had committed offset `1` on partition 0 and `0` on partition 1; user-b's had `0` on partition 0 and `1` on partition 1). `GROUP_ID` and `TRANSACTIONAL_ID` are isolated; topic names are not yet.

**7. Revert:**

```bash
oc apply -f deploy/proxy/05-virtualkafkacluster.yaml
```

> ⚠️ **While the demo wiring is active**, the virtual cluster runs only `sasl-inspection` + `entity-isolation` — the **ACLs and record-validation filters are bypassed**, and other proxy clients whose principals contain `-` (e.g. the live Camel producer/consumer Deployments, whose principals are `sub` UUIDs) are rejected with the mapping error and stop delivering. On revert the producer recovers by itself; the consumer's route terminates permanently ([BridgeErrorStrategy](02-record-validation.md)) and needs a pod restart: `oc -n kafka-consumer delete pod -l app=camel-kafka-consumer`. Note also that during the demo the users can use *any* group name (user-b normally only has `kafka-user-b-group`) — the isolation mapping is demonstrated precisely because the ACLs are out of the way.
>
> ⚠️ **Topic names are not mapped by this filter** — EntityIsolation isolates `GROUP_ID` and `TRANSACTIONAL_ID` only. There is no built-in way to map one client-visible topic name to a different broker-side name at the proxy: the proxy rewrites broker *addresses*, not topic names, and arbitrary topic mapping ("topic weaving") is still an upstream design proposal (a custom filter could do it, but must keep the Metadata/Produce/Fetch flow and the topic-id correspondence consistent).
