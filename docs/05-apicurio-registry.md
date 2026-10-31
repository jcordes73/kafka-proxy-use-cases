# Step 5: Deploy Apicurio Registry and Validate Records (Record Validation)

> [↑ Setup overview](../openshift-setup.md) · prev: [Step 4: MirrorMaker 2](04-mirrormaker2.md) · next: [Step 6: Proxy](06-proxy.md)

The **Record Validation filter** (Technology Preview in 3.2 — see the Red Hat [Record Validation filter guide](https://docs.redhat.com/en/documentation/red_hat_streams_for_apache_kafka/3.2/html/record_validation_filter_guide)) blocks poison messages at the proxy: records that fail validation are rejected and never reach the brokers. Its **schema validation** mode requires the **Red Hat build of Apicurio Registry 3.x** — 2.x support was removed in 3.2, and 3.x identifies schemas by `contentId` (the filter's `apicurioId`; clients still embedding `globalId` fail validation). Only **JSON schemas** are enforced.

This step deploys the registry in a `kafka-registry` namespace — part of the **secure backend**: the DMZ proxy fetches schemas over the in-cluster service, and the app/UI are also exposed via Routes ([5.2](#52--deploy-the-registry-postgresql-storage)) with **Keycloak authentication** (anonymous reads only, so the proxy's unauthenticated schema fetch keeps working) — and registers a demo schema — the validation rule, demo topic, and end-to-end verification moved to the [Demo scenarios](11-demos.md#demo-2--record-validation-end-to-end) document (Demo 2, [Step 11](11-demos.md#step-11-demo-scenarios)). See use case [3 in the guide](../README.md#3-data-quality-blocking-poison-messages-record-validation-filter).

> **Deployment order:** the registry (5.1–5.4) is Step 5 — immediately after MirrorMaker 2 (Step 4) and **before the proxy** (Step 6); see the Guide map in the [setup overview](../openshift-setup.md). It has no dependency on the proxy (the dependency runs the other way: the proxy's schema-validation filter fetches schemas from this registry over the in-cluster HTTP service), so deploying it first means the filter resolves schemas from the moment it is wired. The **Record Validation demo** (demo topic, filter, wiring, verification) lives in [Demo scenarios — Demo 2](11-demos.md#demo-2--record-validation-end-to-end) and deploys with the optional demo scenarios ([Step 11](11-demos.md#step-11-demo-scenarios)).

## 5.1 — Install the Apicurio Registry operator

[`deploy/apicurio/01-subscription.yaml`](../deploy/apicurio/01-subscription.yaml) creates the `kafka-registry` namespace, an OwnNamespace `OperatorGroup`, and a `Subscription` for the `apicurio-registry-3` package (Red Hat Operators catalog, channel `3.2.x` — aligned with the Streams 3.2 LTS line), with Manual approval so OLM does not auto-upgrade the registry:

```bash
oc apply -f deploy/apicurio/01-subscription.yaml

oc -n kafka-registry get installplan          # then approve:
oc -n kafka-registry patch installplan <name> --type merge -p '{"spec":{"approved":true}}'
oc -n kafka-registry wait --for=jsonpath='{.status.phase}'=Succeeded csv/apicurio-registry-3.v3.2.6-r2 --timeout=300s
```

The subscription **pins the exact tested CSV** via `startingCSV: apicurio-registry-3.v3.2.6-r2` (see [Components and Tested Versions](../README.md#components-and-tested-versions) in the README) — Manual approval plus `startingCSV` fixes the operator to the version that has been tested (OLM cannot auto-upgrade it away).

## 5.2 — Deploy the registry (PostgreSQL storage)

Apicurio Registry 3 offers `kafkasql`, `kubernetesops`, `mysql`, and `postgresql` storage. `kafkasql` would reuse the Kafka cluster but requires widening the [Step 3.4](03-kafka-cluster.md#34--restrict-kafka-ingress-to-the-proxy-namespace-dmz) DMZ NetworkPolicy to admit the registry namespace — `postgresql` keeps the DMZ closed. [`deploy/apicurio/02-postgres.yaml`](../deploy/apicurio/02-postgres.yaml) is a dev-only ephemeral PostgreSQL (mirrors the Keycloak pattern from [Step 2.2](02-keycloak.md#22--provision-a-database-dev-ephemeral-postgresql) — data is lost on restart); the DB credentials are applied as a Secret (idempotent via `oc apply`) and referenced from the `ApicurioRegistry3` resource:

```bash
oc apply -f deploy/apicurio/02-postgres.yaml
oc apply -f - <<EOF
apiVersion: v1
kind: Secret
metadata:
  name: registry-postgres-db
  namespace: kafka-registry
type: Opaque
stringData:
  username: registryuser
  password: registrypassword
EOF
oc apply -f deploy/apicurio/03-registry.yaml
oc -n kafka-registry wait --for=condition=Ready apicurioregistry3/apicurio-registry --timeout=300s
```

The `ApicurioRegistry3` resource (note the component-based API — storage config lives under `spec.app`): the registry and UI deployments run without Routes (the proxy fetches schemas over the in-cluster HTTP service), the operator-generated NetworkPolicies (allow-all ingress on 8080/9000) are disabled in favor of the Step 5.3 lockdown, and `spec.app.storage.sql.dataSource` carries the JDBC URL, the username as a plain string, and the password as a Secret reference. Verify the storage backend took effect:

```bash
oc -n kafka-registry logs deploy/apicurio-registry-app-deployment | grep 'Using.*SQL storage'
# Using postgresql SQL storage.
```

### Registry Routes and Keycloak authentication (as deployed here)

The `ApicurioRegistry3` resource (apply with `envsubst` — it embeds `${OPENSHIFT_APPS_DOMAIN}`):

- **Ingress** — `spec.app.ingress` and `spec.ui.ingress` with `enabled: true`, a `host` (`registry-app.${OPENSHIFT_APPS_DOMAIN}` / `registry-ui.${OPENSHIFT_APPS_DOMAIN}`) and `ingressClassName: openshift-default` (the router generates a Route). Without a `host` the operator creates **no** Ingress. The `route.openshift.io/termination: edge` annotation gives the Routes TLS at the router.
- **OIDC authentication** — `spec.app.auth`: `enabled: true`, `authServerUrl` (the Keycloak realm URL from [Step 2.3](02-keycloak.md#23--tls-certificate-and-keycloak-deployment)), `appClientId: registry-api` (the backend client), `uiClientId: registry-ui` (the browser client), `redirectUri` (the UI Route), and **`anonymousReadsEnabled: true`** — the DMZ proxy's Record Validation filter fetches schemas unauthenticated, so reads stay open while writes and the UI login require Keycloak (a write without a token answers 401; the `registry-api` service-account token and the `kafka-admin` user token both work — verified live).
- **Role-based authorization** — `spec.app.auth.authz`: `enabled: true` + `readAccessEnabled: true` (any authenticated user can read; anonymous reads stay open for the proxy). Full access needs the `sr-admin` realm role, assigned to the `kafka-admin` user and to the `service-account-registry-api` service account in the realm import (the SA assignment is done via the admin API after import — see the gotcha below).
- **Keycloak clients** — `registry-api` (confidential) and `registry-ui` (public, redirect `https://registry-ui.${OPENSHIFT_APPS_DOMAIN}/*`) are part of the realm import ([`deploy/keycloak/04-kafka-realm-import.yaml`](../deploy/keycloak/04-kafka-realm-import.yaml), [Step 2.4](02-keycloak.md#24--create-the-kafka-realm-and-the-console-client)).

> ⚠️ **Four gotchas verified live:** (1) the OIDC connection trusts Keycloak's self-signed cert via a **PKCS12 truststore that must be built with `keytool`** — an `openssl pkcs12 -export -nokeys` truststore loads with **0 entries** in the JVM (`trustAnchors parameter must be non-empty` → "OIDC Server is not available" on every authenticated request). Build it in a JDK pod: `oc run keytool-gen --image=registry.access.redhat.com/ubi8/openjdk-17 -n kafka-registry -- bash -c 'keytool -importcert -alias keycloak-ca -file /tmp/ca.crt -keystore /tmp/ts.p12 -storetype PKCS12 -storepass changeit'` with the cert from `keycloak-tls-secret`, then create the Secret as `keycloak-oidc-truststore` (keys `ca.p12` + `ca.password`). (2) the realm import is **skipped when the realm already exists** ("Realm 'kafka' already exists. Import skipped") — after adding the registry clients, delete the realm (Keycloak **master**-realm admin: `/realms/master/.../token` with the `keycloak-initial-admin` credentials) and re-run the import by deleting the `kafka-realm-import` Job; the operator recreates it. (3) **don't declare `service-account-registry-api` as a user in the realm import** — Keycloak auto-creates the service-account user when the client's `serviceAccountsEnabled` is on, and the explicit entry collides (`duplicate key ... (realm_id, username)` → `ModelDuplicateException`, the whole import fails); assign the `sr-admin` role to the auto-created SA via the admin API after the import (`POST /admin/realms/kafka/users/{id}/role-mappings/realm`). (4) **imported users need profile fields** (`firstName`/`lastName`/`email`) — Keycloak 24+ fires the VERIFY_PROFILE required action for users without them and password grants fail with `Account is not fully set up`; the manifest sets them, or clear it via the admin API.

### Getting a token as kafka-admin

The `kafka-admin` user ([Step 2.4](02-keycloak.md#24--create-the-kafka-realm-and-the-console-client), password `openshift`) holds the `sr-admin` role — full registry access. Get a token with a password grant against the `kafka-console` client (which has `directAccessGrantsEnabled: true`; the realm's built-in `admin-cli` only issues a slim token without realm roles, which the role-based authz rejects):

```bash
KAT=$(curl -sk -X POST \
  "https://keycloak-ingress-kafka-auth.${OPENSHIFT_APPS_DOMAIN}/realms/kafka/protocol/openid-connect/token" \
  -d 'grant_type=password&client_id=kafka-console&client_secret=kafka-console-secret&username=kafka-admin&password=openshift' \
  | jq -r .access_token)

curl -sk -X POST "https://registry-app.${OPENSHIFT_APPS_DOMAIN}/apis/registry/v3/groups/default/artifacts" \
  -H "Authorization: Bearer $KAT" -H 'Content-Type: application/json' \
  -d '{"artifactId":"admin-write","artifactType":"JSON","firstVersion":{"content":{"content":"{}","contentType":"application/json"}}}'
# 200 — sr-admin grants full access
```

### Registry URL and credentials

Print the registry REST API URL (used by the proxy's Record Validation filter) and the storage credentials:

```bash
echo "Registry REST API:  http://apicurio-registry-app-service.kafka-registry.svc.cluster.local:8080/apis/registry/v3"
echo "DB username:        $(oc get secret registry-postgres-db -n kafka-registry -o jsonpath='{.data.username}' | base64 --decode)"
echo "DB password:        $(oc get secret registry-postgres-db -n kafka-registry -o jsonpath='{.data.password}' | base64 --decode)"
```

- **URL** — the in-cluster REST service (`apicurio-registry-app-service:8080`, what the DMZ proxy fetches schemas over; the [Step 5.3](#53--lock-down-the-registry-network) lockdown admits the `kafka-proxy` namespace on the REST port) **plus the operator-managed Routes** (`registry-app.${OPENSHIFT_APPS_DOMAIN}`, `registry-ui.${OPENSHIFT_APPS_DOMAIN}` — edge TLS via the `route.openshift.io/termination: edge` annotation; without it the Routes are HTTP-only and HTTPS answers the router's 503).
- **User / password** — the PostgreSQL **storage** credentials (`registry-postgres-db` Secret, dev values `registryuser` / `registrypassword`, referenced by `spec.app.storage.sql.dataSource`). The REST API itself is **authenticated with Keycloak** (below): reads are anonymous (`anonymousReadsEnabled` — the DMZ proxy's schema fetch is unauthenticated), writes require a Bearer token carrying the `sr-admin` role (the `registry-api` service account or the `kafka-admin` user; 401 without a token, 403 without the role). Log into the UI at `https://registry-ui.${OPENSHIFT_APPS_DOMAIN}` with the Keycloak login (`demo` for reads, `kafka-admin` for full access, [Step 8.3](08-console.md#83--recommended-secure-the-console-ui-with-keycloak-oidc)).

## 5.3 — Lock down the registry network

[`deploy/apicurio/04-netpol-dmz.yaml`](../deploy/apicurio/04-netpol-dmz.yaml) mirrors the Kafka DMZ pattern ([Step 3.4](03-kafka-cluster.md#34--restrict-kafka-ingress-to-the-proxy-namespace-dmz)): default-deny ingress to the registry pods, same-namespace traffic (registry ↔ PostgreSQL, UI), ingress from the `kafka-proxy` namespace only on the REST port 8080, and ingress from the OpenShift **router** on 8080 for the [Step 5.2](#52--deploy-the-registry-postgresql-storage) Routes. The router is **hostNetwork** — its traffic is attributed via the `policy-group.network.openshift.io/ingress` label, not the namespace name; a `kubernetes.io/metadata.name: openshift-ingress` selector alone leaves every Route answering the router's 503 (verified live).

```bash
oc apply -f deploy/apicurio/04-netpol-dmz.yaml
```

Verify the lockdown behaves as intended (mirrors the [Step 3.4](03-kafka-cluster.md#34--restrict-kafka-ingress-to-the-proxy-namespace-dmz) test) — from
another namespace the registry is unreachable, from `kafka-proxy` the REST API
answers:

```bash
# From another namespace (e.g., default): connection times out
oc run registry-blocked --rm -i --restart=Never --image=curlimages/curl -n default -- \
  -s -o /dev/null -w '%{http_code}' --max-time 10 --connect-timeout 8 \
  http://apicurio-registry-app-service.kafka-registry.svc.cluster.local:8080/apis/registry/v3/system/info
# 000 (connection failed)

# From the kafka-proxy namespace: reachable (this is what the filter uses)
oc run registry-allowed --rm -i --restart=Never --image=curlimages/curl -n kafka-proxy -- \
  -s -w '\nHTTP %{http_code}\n' --max-time 10 \
  http://apicurio-registry-app-service.kafka-registry.svc.cluster.local:8080/apis/registry/v3/system/info
# HTTP 200
```

> The operator's `spec.app.networkPolicy.enabled: false` (and the matching `ui` setting) must be set **before** first deploy — on an existing resource the operator keeps re-creating its allow-all policies. If they appear anyway, delete them and re-apply the CR.

## 5.4 — Register a schema

Schema validation enforces a schema identified by its **`contentId`**. Register a JSON schema through the registry's v3 REST API (from a pod in `kafka-registry` — same-namespace traffic is allowed) and capture the `contentId` from the response:

```bash
echo '{
  "artifactId": "record-validation-demo-value",
  "artifactType": "JSON",
  "firstVersion": {
    "content": {
      "content": "{\"$schema\":\"https://json-schema.org/draft/2020-12/schema\",\"type\":\"object\",\"properties\":{\"orderId\":{\"type\":\"string\"},\"amount\":{\"type\":\"number\"}},\"required\":[\"orderId\",\"amount\"],\"additionalProperties\":false}",
      "contentType": "application/json"
    }
  }
}' | oc run -i --rm registry-curl --restart=Never --image=curlimages/curl -n kafka-registry -- \
  -s -X POST -H 'Content-Type: application/json' --data-binary @- \
  http://apicurio-registry-app-service:8080/apis/registry/v3/groups/default/artifacts

# …"version":{"version":"1", … "contentId":1, …}
export APICURIO_CONTENT_ID=1
```

The demo topic and Record Validation filter that consume this schema moved to the **[Demo scenarios](11-demos.md#demo-2--record-validation-end-to-end)** document (Demo 2) — the demo topic, filter, wiring, and end-to-end verification now deploy with the optional demo scenarios ([Step 11](11-demos.md#step-11-demo-scenarios)).
