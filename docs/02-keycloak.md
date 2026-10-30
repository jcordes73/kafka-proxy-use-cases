# Step 2: Install Red Hat build of Keycloak (kafka-auth namespace)

> [↑ Setup overview](../openshift-setup.md) · prev: [Step 1: User-workload monitoring](01-user-workload-monitoring.md) · next: [Step 3: Kafka cluster](03-kafka-cluster.md)

Deploy the **Red Hat build of Keycloak** as the OIDC authorization server — for the platform's browser-facing components (the **Streams for Apache Kafka Console** login, [Step 8](08-console.md#step-8-deploy-the-streams-for-apache-kafka-console-kafka-console-namespace)) **and for the Kafka data path itself**: the `tls` listener on the Kafka clusters requires SASL/OAUTHBEARER (Step 3.2), and the service-account clients created in Step 2.4 are the workloads' data-path credentials. Based on the [RHBK 26.4 Operator Guide](https://docs.redhat.com/en/documentation/red_hat_build_of_keycloak/26.4/html/operator_guide) (check [access.redhat.com](https://access.redhat.com/support/policy/updates/red_hat_build_of_keycloak_notes) for the latest supported release).

> **Deployment order:** in this guide Keycloak is deployed **second** — after the user-workload monitoring stack (Step 1.1) and before the Kafka cluster (Step 3); see the Guide map in the [setup overview](../openshift-setup.md). Keycloak has no dependency on the Kafka cluster (the data-path dependency runs the other way: the Kafka `tls` listener validates tokens issued by this realm), so deploying it before Kafka works cleanly — the Kafka issuer URL references this deployment's ingress route host.

## 2.1 — Install the Keycloak Operator into `kafka-auth`

The operator is installed via OLM. The ready-to-apply manifest is [`deploy/keycloak/01-kafka-auth-operator.yaml`](../deploy/keycloak/01-kafka-auth-operator.yaml) — it consolidates the `kafka-auth` Namespace, the OperatorGroup, and the Subscription into a single file applied with one `oc apply` (the resources must be applied together so the OperatorGroup exists when the Subscription is validated):

```bash
oc apply -f deploy/keycloak/01-kafka-auth-operator.yaml

oc get subscriptions -n kafka-auth
oc wait --for=jsonpath='{.status.phase}'=Succeeded csv --all -n kafka-auth --timeout=300s
oc get pods -n kafka-auth
```

> `oc apply -f <dir-or-file>` creates the namespace, the OperatorGroup, and subscribes in a single transaction — note that `Namespace`/`OperatorGroup`/`Subscription` resources must be applied together (as in this file) so the OperatorGroup exists when the Subscription is validated.

## 2.2 — Provision a database (dev: ephemeral PostgreSQL)

The operator does **not** manage the database. For development, deploy an ephemeral PostgreSQL — the ready-to-apply manifest is [`deploy/keycloak/02-postgres.yaml`](../deploy/keycloak/02-postgres.yaml) (a `StatefulSet` with `postgres:15`, `emptyDir` storage, and the `postgres-db` Service on 5432, with dev credentials `testuser`/`testpassword`):

```bash
oc apply -f deploy/keycloak/02-postgres.yaml

# Store the DB credentials in a Secret (idempotent — re-running with the same
# values is a no-op instead of failing with AlreadyExists)
oc apply -f - <<EOF
apiVersion: v1
kind: Secret
metadata:
  name: keycloak-db-secret
  namespace: kafka-auth
type: Opaque
stringData:
  username: testuser
  password: testpassword
EOF

# Verify the database is ready before deploying Keycloak (Step 2.3)
oc -n kafka-auth rollout status statefulset/postgresql-db --timeout=180s
oc -n kafka-auth get pods -l app=postgresql-db
```

> **Production:** provision a supported database with persistent storage (cloud offering or a database operator) — not the ephemeral dev setup above.

## 2.3 — TLS certificate and Keycloak deployment

For development, generate a self-signed certificate and deploy Keycloak with an OpenShift-assigned hostname (`ingress.className: openshift-default` lets the operator assign one).

The ready-to-apply manifest is [`deploy/keycloak/03-keycloak.yaml`](../deploy/keycloak/03-keycloak.yaml) (a `Keycloak` CR with 1 instance, PostgreSQL storage, `keycloak-tls-secret`, and the `openshift-default` IngressClass — the operator assigns the Route hostname as `<cr-name>-ingress-<namespace>.<cluster-domain>`).

A second manifest, [`deploy/keycloak/05-keycloak-http.yaml`](../deploy/keycloak/05-keycloak-http.yaml), exposes Keycloak's **plain-http port inside the cluster**: the `Keycloak` CR also sets `http-enabled: true`, but the operator's `keycloak-service` only exposes TLS (8443) and management (9000) — the OAuth chain (the brokers' JWKS fetch and every client's token endpoint, e.g. [Step 6.10](06-proxy.md#610--apply-and-verify) and [Step 9.3](09-camel-clients.md#93--connect-both-clients-to-the-proxy)) uses `http://keycloak-http.kafka-auth.svc.cluster.local:8080/realms/kafka/...` over the in-cluster network. The manifest creates the `keycloak-http` Service plus an additive NetworkPolicy opening port 8080 for the OAuth client namespaces (`kafka-cluster-1`, `kafka-cluster-2`, `kafka-proxy`, `kafka-http-bridge`, `kafka-producer`, `kafka-consumer`, `kafka-console`) — the operator's own `keycloak-network-policy` only admits 8443/9000. **DEV ONLY** — the in-cluster http path is unencrypted; in production, use the TLS route and trust its CA in every OAuth client instead. Apply it together with the Keycloak CR:

The TLS certificate **must carry a SAN for the Route hostname** — the broker-side JWKS fetch validates the hostname against the cert, and a plain CN is not enough (the brokers will fail to start). Discover the hostname the operator will assign first, or generate the cert after the Route exists and replace the Secret:

```bash
source deploy/env.sh   # exports OPENSHIFT_APPS_DOMAIN for the SAN below

openssl req -new -newkey rsa:2048 -nodes \
  -subj '/CN=kafka-auth/O=Kafka Auth./C=US' \
  -addext "subjectAltName=DNS:keycloak-ingress-kafka-auth.${OPENSHIFT_APPS_DOMAIN}" \
  -keyout key.pem -x509 -days 365 -out certificate.pem

# TLS secret from the generated files (idempotent via oc apply)
oc apply -f - <<EOF
apiVersion: v1
kind: Secret
metadata:
  name: keycloak-tls-secret
  namespace: kafka-auth
type: kubernetes.io/tls
stringData:
  tls.crt: |
$(sed 's/^/    /' certificate.pem)
  tls.key: |
$(sed 's/^/    /' key.pem)
EOF
```

Apply and wait for readiness:

```bash
oc apply -f deploy/keycloak/03-keycloak.yaml
oc apply -f deploy/keycloak/05-keycloak-http.yaml

oc wait --for=condition=Ready keycloak/keycloak -n kafka-auth --timeout=600s
oc get keycloaks/keycloak -n kafka-auth \
  -o go-template='{{range .status.conditions}}CONDITION: {{.type}}  STATUS: {{.status}}{{"\n"}}{{end}}'
```

### Keycloak admin

Print the Keycloak admin console URL and the bootstrap admin credentials:

```bash
export OPENSHIFT_APPS_DOMAIN=$(oc get ingress.config.openshift.io cluster \
  -o jsonpath='{.spec.domain}')

echo "Admin console URL:  https://keycloak-ingress-kafka-auth.${OPENSHIFT_APPS_DOMAIN}/admin/"
echo "Username:           $(oc get secret keycloak-initial-admin -n kafka-auth -o jsonpath='{.data.username}' | base64 --decode)"
echo "Password:           $(oc get secret keycloak-initial-admin -n kafka-auth -o jsonpath='{.data.password}' | base64 --decode)"
```

- **URL** — the operator-assigned Route hostname (`<cr-name>-ingress-<namespace>.<cluster-domain>`, discover with `oc get routes -n kafka-auth`); the admin console lives at `/admin/`. The realm URL that clients and the console OIDC login use is `https://keycloak-ingress-kafka-auth.${OPENSHIFT_APPS_DOMAIN}/realms/kafka`.
- **Username / password** — the operator generates the bootstrap admin into the `keycloak-initial-admin` Secret (e.g. `temp-admin` with a random password, lives in the **master** realm). The bootstrap admin manages the realm — it is not used by the data path or the console login. Realm users (both from the realm import): `demo` for the console/registry UI login, and **`kafka-admin` / `openshift`** — the cluster administrator with the `sr-admin` role (full Apicurio Registry access, [Step 5.2](05-apicurio-registry.md#52--deploy-the-registry-postgresql-storage)) and the console `administrators` role ([Step 8.3](08-console.md#83--recommended-secure-the-console-ui-with-keycloak-oidc)); the Kafka cluster itself has no authorization block, so every authenticated principal has full cluster access. **Dev credentials only** — manage real users through your identity source.

> **Production:** change the initial admin credentials, enable MFA, and use a real hostname with a CA-signed certificate. Restrict who can create/edit `Keycloak` and `KeycloakRealmImport` CRs — they effectively hold namespace-level privileges.

## 2.4 — Create the `kafka` realm and the console client

Automate realm creation with a `KeycloakRealmImport` CR — the ready-to-apply manifest is [`deploy/keycloak/04-kafka-realm-import.yaml`](../deploy/keycloak/04-kafka-realm-import.yaml):

```bash
envsubst < deploy/keycloak/04-kafka-realm-import.yaml | oc apply -f -
oc wait --for=condition=Done keycloakrealmimport/kafka-realm-import -n kafka-auth --timeout=300s
```

Field notes (full manifest in the deploy file — apply with `envsubst`, it embeds the `${OPENSHIFT_APPS_DOMAIN}` redirect URI):

- `realm.realm: kafka` + `accessTokenLifespan: 300` — short-lived access tokens (5 min), so access stops quickly on revocation.
- `clients`:
  - `kafka-console` — browser login (standard flow) for the Console UI ([Step 8](08-console.md#step-8-deploy-the-streams-for-apache-kafka-console-kafka-console-namespace)); redirect URI points at the console's hostname.
  - `kafka-console-kafka`, `kafka-bridge`, `camel-producer`, `camel-consumer`, `demo-client` — **service-account clients** (`client_credentials`) that are the data-path credentials for the OAUTHBEARER-only `tls` listener ([Step 3.2](03-kafka-cluster.md#32--create-the-kafka-cluster), [Step 2.5](#25--client-authentication-at-the-proxy)).

Where the clients are used:

| Client | Used by | Where |
|---|---|---|
| `kafka-console` | Console UI (OIDC browser login, standard flow) | [Step 8.3](08-console.md#83--recommended-secure-the-console-ui-with-keycloak-oidc) |
| `kafka-console-kafka` | Console's Kafka connection (OAUTHBEARER) | [Step 8.2](08-console.md#82--deploy-the-console-connected-to-the-virtual-cluster) |
| `kafka-bridge` | HTTP Bridge Kafka connection | [Step 7.2](07-http-bridge.md#72--deploy-the-bridge-with-a-kafkabridge-resource) |
| `camel-producer` / `camel-consumer` | Camel producer/consumer Kafka connections | [Step 9.3](09-camel-clients.md#93--connect-both-clients-to-the-proxy) |
| `registry-api` | Apicurio Registry backend (OIDC token validation; Step 5.2) | [Step 5.2](05-apicurio-registry.md#52--deploy-the-registry-postgresql-storage) |
| `registry-ui` | Apicurio Registry UI (public client, browser login; Step 5.2) | [Step 5.2](05-apicurio-registry.md#52--deploy-the-registry-postgresql-storage) |
| `demo-client` | kcat/CLI smoke tests | [Step 6.10](06-proxy.md#610--apply-and-verify) |

The realm import does **not** create Kubernetes Secrets — create one per workload in its own namespace before deploying the clients (see [`deploy/README.md`](../deploy/README.md)).

> **Re-importing an existing realm:** the operator's import job runs Keycloak's startup import (`kc.sh import`), which **skips an existing realm entirely** — so re-applying the CR with changed clients does *not* update Keycloak (the job reports `Done` without importing). To change clients:
> 1. Delete the realm (via the Keycloak admin API) and delete + re-apply the `KeycloakRealmImport` CR — the fresh import then creates the realm from the CR.
> 2. **Restart the Keycloak pod** — the running instance caches realms in memory and keeps serving the previous state (including old client secrets) until restarted, which surfaces as `invalid_client` from token requests.

Verify the OIDC endpoints are reachable (the same endpoints the console's OIDC login uses):

```bash
oc get routes -n kafka-auth
# Token endpoint: https://<keycloak-hostname>/realms/kafka/protocol/openid-connect/token

# The kafka-console client has directAccessGrantsEnabled: true — password
# grants work for the realm users (demo, kafka-admin). The realm's built-in
# admin-cli accepts the grant too but issues a slim token without realm
# roles, which role-based authorization (Step 5.2) rejects.
curl -s https://<keycloak-hostname>/realms/kafka/protocol/openid-connect/token \
  -d grant_type=password \
  -d client_id=kafka-console -d client_secret=kafka-console-secret \
  -d username=demo -d password=demopassword | jq -r .access_token

# Cluster administrator token (sr-admin role — full registry access):
curl -s https://<keycloak-hostname>/realms/kafka/protocol/openid-connect/token \
  -d grant_type=password \
  -d client_id=kafka-console -d client_secret=kafka-console-secret \
  -d username=kafka-admin -d password=openshift | jq -r .access_token
```

> The client's `secret` above is defined inline for simplicity. In production, store client secrets in Keycloak (rotated there) or in a Secret-backed vault rather than committing them in YAML — and keep the `kafka-auth` namespace access tightly restricted.

## 2.5 — Client authentication at the proxy

The proxy supports client authentication, and identity-aware filters (`Authorization`, `EntityIsolation`) require it — without authentication the proxy's subject is anonymous. The options — mTLS is documented in the Red Hat [Deploying and Managing guide](https://docs.redhat.com/en/documentation/red_hat_streams_for_apache_kafka/3.2/html/deploying_and_managing_streams_for_apache_kafka_proxy_on_openshift) (Securing a proxy → mutual TLS), SASL Passthrough Inspection in the Red Hat [SASL Inspection filter guide](https://docs.redhat.com/en/documentation/red_hat_streams_for_apache_kafka/3.2/html/sasl_inspection_filter_guide); the upstream [Client Authentication guide](https://kroxylicious.io/documentation/0.25.1/html/authentication-guide) additionally documents SASL Passthrough, OAuthBearer Validation, and SASL Termination:

| Approach | Filter | Broker keeps auth authority? | Infrastructure |
|---|---|---|---|
| mTLS | — (virtual cluster `tls.trust`) | — | PKI for client certificates |
| SASL Passthrough *(default)* | none | Yes — ACLs, audit logging, quotas keep working | None |
| SASL Passthrough Inspection | `SaslInspection` | Yes — proxy extracts the end-client principal (OAUTHBEARER, SCRAM-SHA-256/512 enabled by default; PLAIN opt-in) | None |
| OAuthBearer Validation | `OauthBearerValidation` | Yes — proxy rejects invalid/expired JWTs (JWKS, audience, issuer) before they reach the broker | JWKS endpoint |
| SASL Termination | `SaslTermination` | No — proxy authenticates the client itself (OAUTHBEARER or SCRAM) | JWKS endpoint or SCRAM credential store |

**This deployment authenticates the data path end-to-end with OAuth.** The `tls` listener (9093) — the proxy's upstream — requires SASL/OAUTHBEARER (`custom` auth with the Strimzi OAuth callback handlers, OAUTHBEARER as the only mechanism — see [Step 3.2](03-kafka-cluster.md#32--create-the-kafka-cluster) and [deploy/kafka/02-kafka.yaml](../deploy/kafka/02-kafka.yaml)). The proxy relays the client's SASL/OAUTHBEARER exchange to the brokers (SASL passthrough) — the proxy cannot authenticate to the brokers itself (no `KafkaService` SASL support), so **every client of the virtual cluster must authenticate with OAUTHBEARER**:

- **Tokens from Keycloak:** workloads obtain short-lived tokens (5 min, `accessTokenLifespan: 300`) with the `client_credentials` grant from their service-account client in the `kafka` realm ([deploy/keycloak/04-kafka-realm-import.yaml](../deploy/keycloak/04-kafka-realm-import.yaml)). The service account's `preferred_username` becomes the Kafka principal (`OAuthKafkaPrincipalBuilder`). Disable a client in Keycloak and access stops at the next token expiry.
- **Broker-side validation:** the brokers validate each JWT against Keycloak's JWKS endpoint (internal http service) — no per-request calls to Keycloak. `inter.broker.listener.name` stays `plain` (a non-OAuth listener). Hardening option: set `oauth.check.audience="true"` on the listener and add an audience mapper to each client so tokens issued for other services are rejected.
- **Broker isolation still comes from the DMZ** ([Step 3.4](03-kafka-cluster.md#34--restrict-kafka-ingress-to-the-proxy-namespace-dmz)): Kafka ingress is denied to everything except the `kafka-proxy` namespace, on the client-facing listener ports only. Treat proxy access as broker access.
- **Identity-aware proxy filters** (`Authorization`, `EntityIsolation`) need an authenticated subject at the proxy — add a `SaslInspection` filter (OAUTHBEARER is enabled by default) to put the end-client principal in front of the proxy's Authorization filter, and `OauthBearerValidation` to reject invalid/expired tokens before they reach the brokers (the proxy must then trust Keycloak's TLS certificate to fetch the JWKS).

The alternative — SASL/PLAIN with `scram-sha-512` `KafkaUser` credentials — is a valid, simpler option if you don't need token-based identity: swap the listener's `authentication` for `type: plain` and provision `KafkaUser` resources instead.

**The OAuth data path is implemented in the ready-to-apply manifests** (use case [5 in the guide](../README.md#5-oidc--oauth-20-authentication-with-red-hat-build-of-keycloak)):

1. **Broker listener** — [deploy/kafka/02-kafka.yaml](../deploy/kafka/02-kafka.yaml) configures the `tls` listener with `custom` auth + the Strimzi OAuth callback handlers (`sasl.enabled.mechanisms: OAUTHBEARER` only, `inter.broker.listener.name: plain`).
2. **Keycloak clients** — [deploy/keycloak/04-kafka-realm-import.yaml](../deploy/keycloak/04-kafka-realm-import.yaml) creates the per-workload service-account clients (client_credentials grant, 5-minute tokens). Create a Kubernetes Secret per workload for the client secret before deploying the clients (the realm import does not create Secrets).
3. **Clients** — the bridge uses `custom` authentication with the Strimzi OAuth client handler ([deploy/bridge/01-kafka-bridge.yaml](../deploy/bridge/01-kafka-bridge.yaml)), the Camel clients use Camel's `saslAuthType=OAUTH` (Apache Kafka's native handler), and the smoke-test CLI authenticates with the native handler — all documented in their respective steps.
4. **(Optional) identity-aware filters** — add `KafkaProtocolFilter` resources in `kafka-proxy` and wire them to the virtual cluster with `spec.filterRefs` — e.g. `SaslInspection` (OAUTHBEARER enabled by default), plus `OauthBearerValidation` with `jwksEndpointUrl` pointing at Keycloak's `…/realms/kafka/protocol/openid-connect/certs`:

   ```yaml
   kind: KafkaProtocolFilter
   metadata:
     name: sasl-inspection
     namespace: kafka-proxy
   spec:
     type: SaslInspection
     configTemplate:
       enabledMechanisms: [ "OAUTHBEARER" ]
   ---
   kind: KafkaProtocolFilter
   metadata:
     name: oauthbearer-validation
     namespace: kafka-proxy
   spec:
     type: OauthBearerValidation
     configTemplate:
       jwksEndpointUrl: https://<keycloak-hostname>/realms/kafka/protocol/openid-connect/certs
       expectedIssuer: https://<keycloak-hostname>/realms/kafka
   ```

   The proxy must trust Keycloak's TLS certificate to fetch the JWKS (mount the CA from Step 2.3 into the proxy pod via the `KafkaProxy` resource's pod template, or use a CA-signed certificate for Keycloak).

> An OAuth-secured Kafka cluster *outside* the proxy path (clients connect directly) remains a valid alternative — the same `custom` authentication configuration applies, without the filter step.
