# Step 8: Deploy the Streams for Apache Kafka Console (kafka-console namespace)

> [↑ Setup overview](../openshift-setup.md) · prev: [Step 6: Proxy](06-proxy.md) · next: [Step 9: Camel clients](09-camel-clients.md)

The **Streams for Apache Kafka Console** is a web UI for inspecting Kafka clusters — topics, nodes, consumer groups, and metrics. Since 3.2, the console can connect to **Streams for Apache Kafka Proxy virtual clusters**: point it at the proxy's clusterIP bootstrap via `kafkaClusters.kind: virtualkafkaclusters.kroxylicious.io` and it appears in the cluster selector with a **Virtual** label. In this DMZ layout the console connects **through the proxy** — direct access from `kafka-console` to the Kafka pods is denied by the [Step 3.4](03-kafka-cluster.md#34--restrict-kafka-ingress-to-the-proxy-namespace-dmz) NetworkPolicies. Based on the [Using the Streams for Apache Kafka Console](https://docs.redhat.com/en/documentation/red_hat_streams_for_apache_kafka/3.2/html/using_the_streams_for_apache_kafka_console) guide.

> **Deployment order:** the console is deployed **eighth** in this guide — immediately after the HTTP Bridge (Step 7) and right after the proxy it connects through (its `kafkaClusters` entry points at the proxy's virtual cluster, with OIDC against the Keycloak deployment from Step 2); see the Guide map in the [setup overview](../openshift-setup.md).

## 8.1 — Install the console operator

The operator installs with **cluster-wide scope** (a single instance manages consoles in all namespaces). As deployed here, the operator comes from the **Red Hat OLM artifact**: package `amq-streams-console` in the Red Hat Operators catalog, channel `amq-streams-3.2.x` (CSV `amq-streams-console.v3.2.2-3`). The package supports **AllNamespaces installs only**, so the OperatorGroup is global (no `targetNamespaces`) while living in `kafka-console`. [`deploy/console/01-console-operator.yaml`](../deploy/console/01-console-operator.yaml) declares the namespace, OperatorGroup, and Subscription in one transaction:

```bash
oc apply -f deploy/console/01-console-operator.yaml

# installPlanApproval is Manual — approve the InstallPlan deliberately:
oc get installplan -n kafka-console
oc -n kafka-console patch installplan <install-plan-name> --type merge \
  -p '{"spec":{"approved":true}}'

oc wait --for=jsonpath='{.status.phase}'=Succeeded \
  csv/amq-streams-console.v3.2.2-3 -n kafka-console --timeout=300s
oc get deployments -n kafka-console
```

Expected output:

```text
NAME                                    READY  UP-TO-DATE  AVAILABLE
amq-streams-console-operator-v3.2.2-3   1/1    1           1
```

The OLM operator Deployment name embeds the CSV version (`amq-streams-console-operator-v3.2.2-3`) and changes on upgrades. The Red Hat operator ships the console itself as Red Hat build images (`registry.redhat.io/amq-streams/console-api-rhel9`, `console-ui-rhel9`) — it leaves an existing console Deployment untouched (running whatever images it had); delete the console Deployment and let the operator recreate it to move onto the Red Hat build images.

> **Migrating from a non-OLM console operator install?** Two gotchas hit during this guide's own migration: (1) the OLM ServiceAccount is **also named `streamshub-console-operator`** — if you delete the old install's resources by name, you take out the OLM operator's SA too and it fails leader election with `401 Unauthorized` on lease `streamshub-console-operator-lease` until the SA is recreated; (2) the OLM operator drives exposure through a `networking.k8s.io/Ingress` (the OpenShift router generates the Route from it) — a leftover Route from the old install keeps the hostname claimed (`HostAlreadyClaimed`) and the operator stalls with `DependentsNotReady: ConsoleIngress` until the stale Route is deleted.

Alternatives:

- **Upstream release artifacts (non-OLM):** `oc apply -f` the upstream release install file [`streamshub-console-operator.yaml`](https://github.com/streamshub/console/releases/tag/0.14.2) with `${NAMESPACE}` resolved to `kafka-console` — installs operator `0.14.2` with Deployment `streamshub-console-operator` and creates the CRDs itself.

## 8.2 — Deploy the Console connected to the virtual cluster

The ready-to-apply manifest is [`deploy/console/02-kafka-console.yaml`](../deploy/console/02-kafka-console.yaml) — apply it with `envsubst` (it embeds the `${OPENSHIFT_APPS_DOMAIN}` hostname):

```bash
envsubst < deploy/console/02-kafka-console.yaml | oc apply -f -
oc -n kafka-console rollout status deploy/kafka-console-console-deployment --timeout=180s

# The UI answers 200 on the hostname from spec.hostname (307 to the
# sign-in flow once Step 8.3 adds the OIDC login):
curl -sk -o /dev/null -w '%{http_code}\n' https://kafka-console.${OPENSHIFT_APPS_DOMAIN}
```

Field notes (full manifest in the deploy file):

- `spec.hostname` — the console's Route hostname (`kafka-console.${OPENSHIFT_APPS_DOMAIN}`).
- `spec.kafkaClusters[]` — connects to the **proxy's virtual cluster** (`kind: virtualkafkaclusters.kroxylicious.io`, `namespace: kafka-proxy`), never the brokers (the Step 3.4 DMZ would deny direct access anyway). To connect to a physical cluster instead, use the Strimzi resource shape.
- `spec.kafkaClusters[].properties.values` — the Kafka client properties for the connection: `bootstrap.servers` (the proxy's clusterIP bootstrap, operator-allocated port) plus the **SASL/OAUTHBEARER** settings — the proxied `tls` listener requires it (Step 3.2), so the console authenticates with the `kafka-console-kafka` Keycloak service-account client via the native OAuth handler. Create the Secret first (`oc create secret generic kafka-console-kafka-oauth --from-literal=client-secret=kafka-console-kafka-secret -n kafka-console` — the realm import does not create Secrets).
- `spec.kafkaClusters[].properties.valuesFrom` — TLS client props from the `console-kafka-tls` ConfigMap (the proxy CA as a PEM trust store; Step 8.2).
- Kafka 4.x rejects **http** token endpoints unless allowlisted — the console JVM needs `-Dorg.apache.kafka.sasl.oauthbearer.allowed.urls=<token-endpoint-url>`; if the Console CR cannot set extra JVM options in your version, point `sasl.oauthbearer.token.endpoint.url` at the Keycloak **https** route instead (and ensure the console JVM trusts Keycloak's CA, as with the OIDC trust store below).
- **Metrics:** with no metrics source configured, the operator creates an **embedded Prometheus** (development/evaluation only). For production, use OpenShift's user workload monitoring or a standalone Prometheus via `metricsSources` — see [Step 1.7](01-user-workload-monitoring.md#17--console-kafka-console-read-metrics-from-user-workload-monitoring) for the exact configuration — and note the Kafka cluster must expose the metrics properties matching the example `console-kafka-metrics` ConfigMap from the console artifacts for the console to display all data.

When the console is running, access the UI at the hostname from `spec.hostname` (the operator exposes it over HTTP).

## 8.3 — (Recommended) Secure the console UI with Keycloak (OIDC)

The console supports OIDC for its own UI access — the browser login goes through the Red Hat build of Keycloak realm from [Step 2](02-keycloak.md#step-2-install-red-hat-build-of-keycloak-kafka-auth-namespace), using the `kafka-console` client (standard flow) created there. The ready-to-apply manifest [`deploy/console/02-kafka-console.yaml`](../deploy/console/02-kafka-console.yaml) already includes the `security.oidc` block:

- `security.oidc.authServerUrl` / `clientId` — the Keycloak realm URL (`https://keycloak-ingress-kafka-auth.${OPENSHIFT_APPS_DOMAIN}/realms/kafka`) and the `kafka-console` client.
- `security.oidc.clientSecret` — from the `kafka-console-oidc-secret` Secret (key `client-secret`).
- `security.oidc.trustStore` — Keycloak's served certificate is self-signed (Step 2.3) — the console JVM needs it as a trust anchor, or OIDC discovery fails with "OIDC server is not available" (PKIX path building failed) and the UI answers 500 on every request. The `keycloak-ingress-ca` ConfigMap is created below.
- `subjects` / `roles` — the `demo` and `kafka-admin` users (realm import) mapped to the `administrators` role, which grants `ALL` on `kafkas`. Log in as `kafka-admin` / `openshift` for full console administration ([Step 2.3](02-keycloak.md#23--tls-certificate-and-keycloak-deployment)).

Store the client secret from [Step 2.4](02-keycloak.md#24--create-the-kafka-realm-and-the-console-client) in a Secret, and publish Keycloak's TLS
certificate as a ConfigMap for the trust store above (both idempotent via `oc apply`):

```bash
oc apply -f - <<EOF
apiVersion: v1
kind: Secret
metadata:
  name: kafka-console-oidc-secret
  namespace: kafka-console
type: Opaque
stringData:
  client-secret: kafka-console-secret
EOF

oc get secret keycloak-tls-secret -n kafka-auth -o jsonpath='{.data.tls\.crt}' | base64 -d > keycloak-ca.crt
oc apply -f - <<EOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: keycloak-ingress-ca
  namespace: kafka-console
data:
  ca.crt: |
$(sed 's/^/    /' keycloak-ca.crt)
EOF
```

Apply and verify the login flow is wired correctly — the console re-deploys,
and an unauthenticated request must end up **redirected to Keycloak**, not
answered with 500. The Red Hat build guards the UI with a NextAuth-style
flow: a plain GET is answered `307 → /api/auth/signin`, and reaching Keycloak
takes a CSRF-token POST:

```bash
envsubst < deploy/console/02-kafka-console.yaml | oc apply -f -
oc -n kafka-console rollout status deploy/kafka-console-console-deployment --timeout=180s

oc -n kafka-console logs deploy/kafka-console-console-deployment | grep 'OIDC server is not available'
# (no output — the console reached Keycloak for OIDC discovery)

curl -sk -o /dev/null -w '%{http_code} -> %{redirect_url}\n' https://<console-hostname>
# 307 -> https://<console-hostname>/api/auth/signin?callbackUrl=%2F

CSRF=$(curl -sk -c /tmp/console-csrf.txt https://<console-hostname>/api/auth/csrf \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["csrfToken"])')
curl -sk -b /tmp/console-csrf.txt -o /dev/null -w '%{http_code} -> %{redirect_url}\n' \
  -X POST --data-urlencode "csrfToken=$CSRF" \
  https://<console-hostname>/api/auth/signin/oidc
# 302 -> https://<keycloak-hostname>/realms/kafka/protocol/openid-connect/auth?client_id=kafka-console&...
```

### Console login

With the OIDC flow verified, print the console URL and the login credentials:

```bash
export OPENSHIFT_APPS_DOMAIN=$(oc get ingress.config.openshift.io cluster \
  -o jsonpath='{.spec.domain}')

echo "Console URL:  https://kafka-console.${OPENSHIFT_APPS_DOMAIN}"
echo "Username:     demo"
echo "Password:     demopassword"
```

- **URL** — the console's Route hostname, `spec.hostname` = `kafka-console.${OPENSHIFT_APPS_DOMAIN}` (verify against the live Route with `oc get routes -n kafka-console`).
- **User / password** — the `demo` user (`demopassword`) for read-along demos, or `kafka-admin` (`openshift`) for full administration, both from the Keycloak realm import ([`deploy/keycloak/04-kafka-realm-import.yaml`](../deploy/keycloak/04-kafka-realm-import.yaml)); both are mapped to the `administrators` role by the `security.oidc.subjects` block above, which grants `ALL` on the virtual cluster. **Dev credentials only** — the password is committed here by design; manage real users through your identity source and never reuse these values outside a sandbox.

> Group-based authorization (mapping a JWT claim such as `groups` to roles via `subjects[].claim`) requires group membership claims in the Keycloak tokens — configure the realm accordingly, or map individual users as shown. See the console guide's [OIDC security section](https://docs.redhat.com/en/documentation/red_hat_streams_for_apache_kafka/3.2/html/using_the_streams_for_apache_kafka_console/assembly-deploying-str#ref-authentication-options-str) for the full `roles`/`subjects` syntax, including per-cluster role rules.

## 8.4 — Operational notes

- **DMZ coherence:** the console connects only to the proxy's virtual cluster — no direct broker access from `kafka-console`, consistent with the [Step 3.4](03-kafka-cluster.md#34--restrict-kafka-ingress-to-the-proxy-namespace-dmz) lockdown. The console sees the *virtual* cluster (with a **Virtual** label in the selector), so topics and records you browse are exactly what proxy clients see.
- **OIDC-secured console:** the console UI is protected by `security.oidc` (Step 8.3) — browser users log in through Keycloak with the printed sandbox credentials (`demo` / `demopassword`, see Console login above). The console's Kafka connection to the virtual cluster also authenticates with OAUTHBEARER ([Step 2.5](02-keycloak.md#25--client-authentication-at-the-proxy)) — tokens from the `kafka-console-kafka` service-account client, via the `kafkaClusters.properties` SASL settings in 13.2.
- **Namespace layout:** the console joins the DMZ family of namespaces — `kafka` (brokers, locked down) → `kafka-proxy` (proxy) → `kafka-http-bridge` (HTTP API) → `kafka-console` (UI), with `kafka-auth` (Keycloak) providing the console's identity layer and `kafka-registry` (Apicurio Registry, [Step 5](05-apicurio-registry.md#step-5-deploy-apicurio-registry-and-validate-records-record-validation)) backing the proxy's Record Validation filter.
