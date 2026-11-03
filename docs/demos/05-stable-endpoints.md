# Demo 5: Stable endpoints

> [↑ Demo scenarios](../10-demos.md) · prev: [Demo 4: DMZ pivot attempt](04-dmz-pivot.md) · next: [Demo 6: Authorization at the proxy](06-proxy-authorization.md)

Clients never see broker topology changes:

```bash
# Roll every broker of the cluster the proxy targets
oc rollout restart deployment -n kafka-cluster-1 || oc delete pod -l strimzi.io/cluster=kafka-cluster-1 -n kafka-cluster-1

# The Camel producer (timer-driven) keeps producing through the proxy with no
# errors during the roll — its bootstrap is the proxy's clusterIP bootstrap
oc logs -f deployment/camel-kafka-producer -n kafka-producer
```

## Proxy ACLs (who may do what, as deployed here)

Access to the virtual clusters is decided by the proxy's Authorization filter ([deploy/proxy/06-kafka-acl.yaml](../../deploy/proxy/06-kafka-acl.yaml) — the `kafka-acl-rules` ConfigMap), deny-by-default: **everything not explicitly allowed falls to `otherwise deny`** (including `demo-client`). Every grant carries **both the client name and its current `sub` UUID** — SaslInspection derives the principal from the JWT `sub` claim, and the UUIDs change whenever the `kafka` realm is re-imported. DESCRIBE is required alongside READ/WRITE (producers and consumers fetch Metadata first — without it every request is denied with `TOPIC_AUTHORIZATION_FAILED`, verified live):

| Principal | Topics | Operations | Consumer groups |
|---|---|---|---|
| `service-account-kafka-bridge`, `service-account-kafka-console-kafka`, `kafka-admin` (+ sub UUIDs) | `*` | `*` (also `*` on groups and transactional IDs) | `*` |
| `service-account-camel-producer` (+ sub UUID) | `topic-A` only | `WRITE, DESCRIBE` | — |
| `service-account-camel-consumer` (+ sub UUID) | `topic-A` only | `READ, DESCRIBE` | `camel-consumer` (READ) |
| `kafka-user-a` (+ sub UUID) | `topic-A` only | `READ, WRITE, DESCRIBE` | `kafka-user-a-group` (READ) |
| `kafka-user-b` | `topic-A` | `READ` | `kafka-user-b-group` (READ) |
| `kafka-user-c` (+ sub UUID) | `topic-C` only | `READ, WRITE, DESCRIBE` | `kafka-user-c-group` (READ) |

`topic-B` is infrastructure-only — the deny-path topic for [Demo 6](06-proxy-authorization.md).

## Load test: 1000 messages through the proxy as `kafka-user-a` (kafka-cli, OAuth)

The same stable endpoint also carries load: a throwaway Kafka CLI pod produces and consumes **1000 messages** over the proxy with OAUTHBEARER authentication **as the realm user `kafka-user-a`** — writing to `topic-A` and reading back through its consumer group (verified live: ~2s produce, ~6s consume, 1000/1000 received). The CLI talks to the proxy's `clusterIP` ingress, which is **TLS** — so the client must use `SASL_SSL` with the proxy CA in its truststore. `kafka-user-a` is a Keycloak **password user** (not a service account), so the test uses Strimzi's OAuth client handler — `JaasClientOauthLoginCallbackHandler` (shipped in the Strimzi Kafka image) — which supports the **password grant** (`oauth.password.grant.username`/`oauth.password.grant.password`) against the `kafka-console` client, exactly how the realm import intends these users to authenticate:

```bash
# 1. Extract the proxy CA (the proxy-tls Secret is self-signed — it is its own CA,
#    and its wildcard SAN covers every virtual cluster's bootstrap/broker names)
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

# 2. Run a Kafka CLI pod with the CA mounted (overrides must come before the `--` separator)
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

# 3. Produce and consume 1000 messages through the proxy as kafka-user-a.
#    The produced records must be schema-valid (the record-validation filter sits
#    on kafka-cluster-1): {"orderId": "...", "amount": <number>}. The consumer
#    must join kafka-user-a-group — the only group the ACLs grant user-a.
oc -n kafka-proxy exec kafka-cli -- bash -c '
    cat > /tmp/client.properties <<EOF
security.protocol=SASL_SSL
sasl.mechanism=OAUTHBEARER
sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required oauth.token.endpoint.uri="http://keycloak-http.kafka-auth.svc.cluster.local:8080/realms/kafka/protocol/openid-connect/token" oauth.client.id="kafka-console" oauth.client.secret="kafka-console-secret" oauth.password.grant.username="kafka-user-a" oauth.password.grant.password="password-a";
sasl.login.callback.handler.class=io.strimzi.kafka.oauth.client.JaasClientOauthLoginCallbackHandler
ssl.truststore.type=PEM
ssl.truststore.location=/opt/proxy-ca/ca.crt
EOF
    B=kafka-cluster-1-cluster-ip-1-bootstrap.kafka-proxy.svc.cluster.local:9292
    seq 1 1000 | sed "s/^/{\"orderId\":\"cli-user-a-/;s/$/\",\"amount\":42}/" | bin/kafka-console-producer.sh \
      --bootstrap-server $B --topic topic-A --command-config /tmp/client.properties
    bin/kafka-console-consumer.sh --bootstrap-server $B --topic topic-A --from-beginning \
      --max-messages 1000 --timeout-ms 60000 --group kafka-user-a-group \
      --command-config /tmp/client.properties | wc -l   # 1000
'
oc -n kafka-proxy delete pod kafka-cli
```

> **Verified gotchas (all hit live while building this test):**
> - **`security.protocol=SASL_SSL`, not `SSL` and not `SASL_PLAINTEXT`.** The `clusterIP` ingresses are `protocol: TLS` (external port 9292 → the proxy's TLS listener on 9291) and require SASL **over** TLS. `SASL_PLAINTEXT` fails the TLS handshake (`Connection terminated during authentication`); plain `SSL` skips SASL entirely — the proxy's SaslInspection filter then closes the connection with *"Client attempted METADATA request without having attempted SASL authentication"*.
> - **A truststore file path goes in `ssl.truststore.location`, not `ssl.truststore.certificates`** — the latter is an inline PEM *value*; a path there fails with `No matching CERTIFICATE entries in PEM file` (Kafka parses the literal path string as PEM content).
> - **The Kafka CLI's native OAuth handler cannot authenticate a password user.** `OAuthBearerLoginCallbackHandler` only does the client_credentials grant — authenticating as `kafka-user-a` needs Strimzi's `JaasClientOauthLoginCallbackHandler` (in the Strimzi Kafka image) with the `oauth.password.grant.username`/`oauth.password.grant.password` JAAS options. With the wrong option names the handler silently falls back to client_credentials and Keycloak answers 401 `unauthorized_client` (`kafka-console` has `serviceAccountsEnabled: false`).
> - **`kafka-user-a`'s tokens had no `sub` claim** — the live realm's `basic` client scope carried a **property mapper** against a `sub` *user attribute* (which no Keycloak user has) instead of the built-in `oidc-sub-mapper`; every produce then failed with *"Proxy cannot extract authorizationId from SASL authenticate request"*. The realm import manifest defines the correct mapper; the live realm was fixed via the Keycloak admin API to match ([Step 2.4](../02-keycloak.md#24--create-the-kafka-realm-and-the-console-client)).
> - **The ACL grant needs `kafka-user-a`'s `sub` UUID too** (`7a697d11-9bc3-45a5-a4c0-b741c8a0d540`) — with only the name, the connection succeeds but the topic list comes back **empty** (SaslInspection resolves the principal to the `sub` UUID, which matches nothing). The UUID changes whenever the `kafka` realm is re-imported — refresh it in [deploy/proxy/06-kafka-acl.yaml](../../deploy/proxy/06-kafka-acl.yaml) ([ACLs on the proxy layer](../06-proxy.md#acls-on-the-proxy-layer-as-deployed-here)).
> - **Without DESCRIBE, every produce/consume is denied** (`TOPIC_AUTHORIZATION_FAILED`) — producers and consumers fetch Metadata for the topic first. The ACLs grant user-a `{READ, WRITE, DESCRIBE}` on `topic-A`.
> - **The `Expiring credential ... buffer times of 60 and 300 seconds cannot be accommodated` WARN is benign** — the realm's access tokens live 5 minutes ([Step 2.4](../02-keycloak.md#24--create-the-kafka-realm-and-the-console-client)), so the client refreshes early instead; it is not an error.
