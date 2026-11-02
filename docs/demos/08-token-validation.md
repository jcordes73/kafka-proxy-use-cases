# Demo 8: Token validation on the Kafka cluster

> [↑ Demo scenarios](../11-demos.md) · prev: [Demo 7: Entity isolation](07-entity-isolation.md) · next: [Demo 9: Multi-tenant virtual clusters](09-multi-tenant-virtual-clusters.md)

JWTs are validated **only at the Kafka cluster level** — the brokers' `oauth` listener checks each token against Keycloak's JWKS endpoint (audience, issuer, expiry) during the SASL/OAUTHBEARER exchange, and invalid tokens stop at the broker with a SASL error. With the supported Red Hat proxy build, the proxy relays the exchange unchanged (SASL Passthrough) and does not participate in validation.

The demo client authenticates with a short-lived JWT from the `kafka` realm (the Secret created in [Step 2.5](../02-keycloak.md#25--client-authentication-at-the-proxy)):

1. The allow path: a valid demo-client token authenticates and produces normally — the broker validates it against Keycloak's JWKS as part of the SASL exchange.
2. The deny path: forge a token signed with a different key, tamper with one, or use an expired/foreign-issuer token — the **broker** rejects it with a SASL error (e.g. `SASL failed : {"status":"invalid_token"}`) before any produce happens. Run a Kafka CLI pod with the bad token to see it:

   ```bash
   # TAMPERED_TOKEN: a JWT with a modified payload (signature no longer matches)
   oc run kafka-cli --rm -i --restart=Never --image=quay.io/strimzi/kafka:latest-kafka-4.2.2 -n kafka-proxy -- \
     bash -c 'cat > /tmp/client.properties <<EOF
security.protocol=SASL_PLAINTEXT
sasl.mechanism=OAUTHBEARER
sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required \
  oauth.accessToken="$TAMPERED_TOKEN";
sasl.oauthbearer.token.endpoint.url=http://keycloak-http.kafka-auth.svc.cluster.local:8080/realms/kafka/protocol/openid-connect/token
EOF
     echo "tampered" | kafka-console-producer.sh --bootstrap-server kafka-cluster-1-cluster-ip-1-bootstrap.kafka-proxy.svc.cluster.local:9292 --producer.config /tmp/client.properties --topic topic-A'
   ```

3. Revert (only if demo manifests were applied): `oc apply -f deploy/proxy/05-virtualkafkacluster.yaml`

> The demo JWKS/issuer URLs use the internal http Keycloak service — the same endpoint the brokers use. For production, use the Keycloak TLS route and mount its CA into the broker pods (the AMQ Streams oauth listener).
