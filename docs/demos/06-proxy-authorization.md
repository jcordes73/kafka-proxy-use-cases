# Demo 6: Authorization at the proxy

> [↑ Demo scenarios](../10-demos.md) · prev: [Demo 5: Stable endpoints](05-stable-endpoints.md) · next: [Demo 7: Entity isolation](07-entity-isolation.md)

Enforce per-principal ACLs **before requests reach the brokers** — with the platform's **real realm users**. The base platform already carries the ACLs ([deploy/proxy/06-kafka-acl.yaml](../../deploy/proxy/06-kafka-acl.yaml): the `kafka-authorization` filter reads the `kafka-acl-rules` ConfigMap; `SaslInspection` extracts the end-client principal from the SASL/OAUTHBEARER exchange), so this demo needs **no new infrastructure** — a throwaway Kafka CLI pod (as in [Demo 5](05-stable-endpoints.md)) authenticates as `kafka-user-a/b/c` and shows the allow and deny matrix live.

## The matrix (from the deployed ACLs)

| User | Topic-A | Topic-C | Topic-B |
|---|---|---|---|
| `kafka-user-a` | **READ, WRITE** (+ its group) | deny | deny |
| `kafka-user-b` | **READ** only (+ its group) | deny | deny |
| `kafka-user-c` | deny | **READ, WRITE** (+ its group) | deny |

Each user also carries DESCRIBE on its granted topics (Metadata is fetched first) and a scoped consumer-group grant (`kafka-user-{a,b,c}-group`) — consumers join a group, so a topic READ without the group READ fails too.

## Run it

```bash
# 1. Run a Kafka CLI pod with the proxy CA mounted (as in Demo 5)
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

# 2. One client config per user — password grants via the kafka-console client
#    (Strimzi's JaasClientOauthLoginCallbackHandler; the CLI's native OAuth
#    handler cannot authenticate a password user — see Demo 5 gotchas)
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
    mkcfg c kafka-user-C password-c
'
```

Then run the matrix (all commands verified live):

```bash
# 3. ALLOW paths
oc -n kafka-proxy exec kafka-cli -- bash -c '
    B=kafka-cluster-1-cluster-ip-1-bootstrap.kafka-proxy.svc.cluster.local:9292
    # user-a writes topic-A — records must be schema-valid (the record-validation
    # filter sits on kafka-cluster-1): {"orderId": "...", "amount": <number>}
    echo "{\"orderId\":\"demo-a-1\",\"amount\":42}" | bin/kafka-console-producer.sh \
      --bootstrap-server $B --topic topic-A --command-config /tmp/cfg-a.properties
    # user-a reads its own records back through its group
    bin/kafka-console-consumer.sh --bootstrap-server $B --topic topic-A --from-beginning \
      --max-messages 1 --timeout-ms 20000 --group kafka-user-a-group --command-config /tmp/cfg-a.properties
    # user-c writes and reads topic-C (no schema validation there)
    echo "hello-c" | bin/kafka-console-producer.sh \
      --bootstrap-server $B --topic topic-C --command-config /tmp/cfg-c.properties
    bin/kafka-console-consumer.sh --bootstrap-server $B --topic topic-C --from-beginning \
      --max-messages 1 --timeout-ms 20000 --group kafka-user-c-group --command-config /tmp/cfg-c.properties
    # user-b (READ-only) reads topic-A through its group
    bin/kafka-console-consumer.sh --bootstrap-server $B --topic topic-A --from-beginning \
      --max-messages 1 --timeout-ms 20000 --group kafka-user-b-group --command-config /tmp/cfg-b.properties
'

# 4. DENY paths — rejected with TOPIC_AUTHORIZATION_FAILED before the brokers
oc -n kafka-proxy exec kafka-cli -- bash -c '
    B=kafka-cluster-1-cluster-ip-1-bootstrap.kafka-proxy.svc.cluster.local:9292
    echo x | bin/kafka-console-producer.sh --bootstrap-server $B --topic topic-C --command-config /tmp/cfg-a.properties   # user-a → topic-C: denied
    echo x | bin/kafka-console-producer.sh --bootstrap-server $B --topic topic-A --command-config /tmp/cfg-b.properties   # user-b (READ only) → topic-A: denied
    echo x | bin/kafka-console-producer.sh --bootstrap-server $B --topic topic-A --command-config /tmp/cfg-c.properties   # user-c → topic-A: denied
    echo x | bin/kafka-console-producer.sh --bootstrap-server $B --topic topic-B --command-config /tmp/cfg-a.properties   # anyone → topic-B: denied
'
oc -n kafka-proxy delete pod kafka-cli
```

> **Verified live outputs:**
> - **Allow paths:** user-a's schema-valid record lands on `topic-A` and reads back via `kafka-user-a-group`; user-c produces and consumes `topic-C` via `kafka-user-c-group`; user-b consumes `topic-A` via `kafka-user-b-group` (`Processed a total of 1 messages`).
> - **Deny paths:** `TopicAuthorizationException: Not authorized to access topics: [topic-C]` (user-a), `[topic-A]` (user-b, READ-only and user-c), `[topic-B]` (anyone) — the request never leaves the proxy.
> - ⚠️ **The consumer group must match the ACL too:** the console consumer's default group is a random `console-consumer-XXXXX`, which the rules deny — each user must use its scoped `kafka-user-*-group` or the request fails with `GroupAuthorizationException`.
> - ⚠️ **Records to `topic-A` must be schema-valid** — the [record-validation filter](02-record-validation.md) sits on `kafka-cluster-1`; an invalid record is rejected with `InvalidRecordException` before ACLs are even checked. `topic-C` has no validation.

> **Revert:** nothing to revert — the demo uses the base-platform ACLs as deployed ([Step 6.6](../06-proxy.md#66--optional-add-a-filter)). To change the grants, edit [deploy/proxy/06-kafka-acl.yaml](../../deploy/proxy/06-kafka-acl.yaml) and re-apply (the proxy pod rolls on the ConfigMap change). The older demo-scoped variant — a `demo-client` service account with its own ACL ConfigMap ([deploy/demos/06-authorization-filters.yaml](../../deploy/demos/06-authorization-filters.yaml), used by the [walkthrough](../11-demos-walkthrough.md)) — remains available separately.
