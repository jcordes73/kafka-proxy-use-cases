# Demo 1: Token revocation (the OAuth payoff)

> [↑ Demo scenarios](../10-demos.md) · next: [Demo 2: Record Validation end-to-end](02-record-validation.md)

Show that identity is centralized in Keycloak: access stops at the next token expiry when a client is disabled — no credential rotation anywhere in the stack.

```bash
# 1. Get a token for demo-client and produce through the proxy
ACCESS_TOKEN=$(curl -s http://keycloak-http.kafka-auth.svc.cluster.local:8080/realms/kafka/protocol/openid-connect/token \
  -d "client_id=demo-client" -d "client_secret=demo-client-secret" -d "grant_type=client_credentials" | jq -r .access_token)

# 2. Disable the demo-client in Keycloak (admin REST — or use the console UI)
KEYCLOAK_HOST="keycloak-ingress-kafka-auth.${OPENSHIFT_APPS_DOMAIN}"
ADMIN_TOKEN=$(curl -s "https://${KEYCLOAK_HOST}/realms/master/protocol/openid-connect/token" \
  -d "client_id=admin-cli" -d "username=admin" -d "password=<admin-password>" -d "grant_type=password" | jq -r .access_token)
CLIENT_UUID=$(curl -s -H "Authorization: Bearer ${ADMIN_TOKEN}" \
  "https://${KEYCLOAK_HOST}/admin/realms/kafka/clients?clientId=demo-client" | jq -r '.[0].id')
curl -X PUT -H "Authorization: Bearer ${ADMIN_TOKEN}" -H "Content-Type: application/json" \
  -d '{"enabled": false}' "https://${KEYCLOAK_HOST}/admin/realms/kafka/clients/${CLIENT_UUID}"
```

The already-issued token stays valid for its lifetime (5 min — `accessTokenLifespan: 300` in the realm import). The Kafka CLI pod from [Step 6.10](../06-proxy.md#610--apply-and-verify) keeps working until its cached token expires, then fails — **access stops at the next token expiry with zero changes to the stack**. Re-enable the client to restore access.
