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

The subscription **pins the exact tested CSV** via `startingCSV: amq-streams-console.v3.2.2-3` (see [Components and Tested Versions](../README.md#components-and-tested-versions) in the README) — Manual approval plus `startingCSV` fixes the operator to the version that has been tested (OLM cannot auto-upgrade it away).

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
- `spec.kafkaClusters[]` — connects to the **proxy's virtual clusters** (`kind: virtualkafkaclusters.kroxylicious.io`, `namespace: kafka-proxy`) **and to the physical clusters directly** (`kind: kafkas.kafka.strimzi.io` — the default — with `name`/`namespace` pointing at the Strimzi `Kafka` CRs and `listener: tls`). The physical entries let the console show the clusters' **real namespace and status** (the console discovers the `Kafka` CRs via its cluster-wide RBAC — see the gotcha below for why the virtual entries can't). ⚠️ Direct connections **bypass the proxy ACLs** — the brokers have no authorization block, so any authenticated principal has full access; the DMZ netpols ([`deploy/kafka/03-kafka-netpol-dmz.yaml`](../deploy/kafka/03-kafka-netpol-dmz.yaml)) allow `kafka-console` → the **TLS listener only** (9093; the unauthenticated `plain` listener stays proxy/mm2-only), and the `tls` listener's `networkPolicyPeers` in [`deploy/kafka/02-kafka.yaml`](../deploy/kafka/02-kafka.yaml) must carry the matching `kafka-console` peer. The physical connections reuse the same `kafka-console-kafka` client but need **each cluster's own CA** (Strimzi generates a separate cluster CA per `Kafka` CR — copied into `console-kafka-cluster1-ca`/`console-kafka-cluster2-ca` ConfigMaps, `ssl.truststore.certificates` + `ssl.truststore.type=PEM`).
- ⚠️ **The home page's *namespace* column shows `kafka-proxy` and the *status* reads "Not Available" ("Status is only available for Kafka clusters managed by a Strimzi Kafka resource") **for the virtual-cluster entries** — expected, not a config error.** For virtual clusters the console derives the displayed namespace from `kafkaClusters[].namespace` (where the `VirtualKafkaCluster` CR lives), and the *status* is populated only when the console can discover a **Strimzi `Kafka` CR matching the cluster's name + namespace** (`KafkaClusterService.setManaged`/`findCluster`) — it knows nothing about the physical clusters behind the proxy. Creating matching `Kafka` CRs in `kafka-proxy` would deploy real Kafka clusters; and the virtual clusters **cannot** live in the physical clusters' namespaces: every VKC reference (`proxyRef`, `targetKafkaServiceRef`, `filterRefs`, `ingressRef`) is name-only (same-namespace by CRD schema), so the proxy, KafkaService, filters, and ingresses must cohabit in `kafka-proxy`. The **Virtual** label next to the cluster name is the indicator that this is a proxy endpoint — the connections themselves work fully (topics/nodes/groups verified on both). The **physical-cluster entries** (`kind: kafkas.kafka.strimzi.io`) show the real namespace and status. (A side effect of the same behavior: the console's broker metrics queries are scoped to the entry's namespace, so per-broker charts stay empty for the virtual entries — broker metrics remain available through the monitoring stack, [Step 1](01-user-workload-monitoring.md), and the physical entries query the right namespaces.)
- ⚠️ **The Kafka Users page is empty in this stack — expected, by design.** The page is backed by **Strimzi `KafkaUser` CRs** in the cluster's namespace (`KafkaUserService.listUsers` → `getKafkaUsers(namespace, clusterName)`), and none exist here. For the **virtual-cluster entries** the page is *always* empty: `listUsers` requires the console to discover a Strimzi `Kafka` CR backing the context, and there is none in `kafka-proxy` (see the namespace/status gotcha above) — it returns an empty list unconditionally. For the **physical entries** the `Kafka` CR exists but no `KafkaUser` CRs were ever created: the Kafka listeners are **OAUTHBEARER-only (Keycloak)**, so Strimzi `KafkaUser` SCRAM/TLS credentials would be unusable — Kafka users in this stack are **Keycloak realm users** (`kafka-admin`, `kafka-user-A/B/C`, `demo`, managed via the realm import, [Step 2.4](02-keycloak.md#24--create-the-kafka-realm-and-the-console-client)), with access decided by the proxy ACL filter ([Step 6.6](06-proxy.md#66--optional-add-a-filter)). Creating `KafkaUser` CRs just to populate the page would add inert Secrets that can't authenticate against these listeners.
- `spec.kafkaClusters[].properties.values` — the Kafka client properties for the connection: `bootstrap.servers` (the proxy's clusterIP bootstrap, operator-allocated port) plus the **SASL/OAUTHBEARER** settings — the proxied `tls` listener requires it (Step 3.2), so the console authenticates with the `kafka-console-kafka` Keycloak service-account client via the native OAuth handler. Create the Secret first (`oc create secret generic kafka-console-kafka-oauth --from-literal=client-secret=kafka-console-kafka-secret -n kafka-console` — the realm import does not create Secrets).
- `spec.kafkaClusters[].properties.valuesFrom` — TLS client props from the `console-kafka-tls` ConfigMap (the proxy CA as a PEM trust store; Step 8.2).
- Kafka 4.x rejects **http** token endpoints unless allowlisted — the console JVM needs `-Dorg.apache.kafka.sasl.oauthbearer.allowed.urls=<token-endpoint-url>`, set via the Console CR's `spec.containers.api.spec.env` (`JAVA_TOOL_OPTIONS`, verified live). **The entry must be the FULL endpoint URL** — a `scheme://host:port` entry alone does not match and every API call still fails with "The URL cannot be accessed due to restrictions" → "Failed to create new KafkaAdminClient" (error code 5001). Alternatively point `sasl.oauthbearer.token.endpoint.url` at the Keycloak **https** route instead (and ensure the console JVM trusts Keycloak's CA, as with the OIDC trust store below).
- **Metrics:** with no metrics source configured, the operator creates an **embedded Prometheus** (development/evaluation only). For production, use OpenShift's user workload monitoring or a standalone Prometheus via `metricsSources` — see [Step 1.7](01-user-workload-monitoring.md#17--console-kafka-console-read-metrics-from-user-workload-monitoring) for the exact configuration — and note the Kafka cluster must expose the metrics properties matching the example `console-kafka-metrics` ConfigMap from the console artifacts for the console to display all data.

When the console is running, access the UI at the hostname from `spec.hostname` (the operator exposes it over HTTP).

## 8.3 — (Recommended) Secure the console UI with Keycloak (OIDC)

The console supports OIDC for its own UI access — the browser login goes through the Red Hat build of Keycloak realm from [Step 2](02-keycloak.md#step-2-install-red-hat-build-of-keycloak-kafka-auth-namespace), using the `kafka-console` client (standard flow) created there. The ready-to-apply manifest [`deploy/console/02-kafka-console.yaml`](../deploy/console/02-kafka-console.yaml) already includes the `security.oidc` block:

- `security.oidc.authServerUrl` / `clientId` — the Keycloak realm URL (`https://keycloak-ingress-kafka-auth.${OPENSHIFT_APPS_DOMAIN}/realms/kafka`) and the `kafka-console` client.
- `security.oidc.clientSecret` — from the `kafka-console-oidc-secret` Secret (key `client-secret`).
- `security.oidc.trustStore` — Keycloak's served certificate is self-signed (Step 2.3) — the console JVM needs it as a trust anchor, or OIDC discovery fails with "OIDC server is not available" (PKIX path building failed) and the UI answers 500 on every request. The `keycloak-ingress-ca` ConfigMap is created below.
- `subjects` / `roles` — the `kafka-admin` user (realm import) mapped to the `administrators` role. ⚠️ The console's permission model is **two-tier** (verified live): the **global** `security.roles` only governs cluster-level URLs (`/api/kafkas`, `/api/kafkas/<id>`) — a rule with `resources: [kafkas]` does **not** cascade to sub-resources, so with only that rule every topic/group page answers "Not Authorized: Insufficient permissions to resource or action". Topic/group/node access requires **per-cluster** `kafkaClusters[].security.roles` rules (`topics`, `topics/records`, `topics/metrics`, `groups`, `groups/configs`, `nodes`, `nodes/configs`, `nodes/metrics`, `rebalances`, `users`) — the console builds those possessed permissions as `kafkas/<clusterId>/<subresource>`, which is what the per-cluster URLs check against. Also on the 3.2.2 operator: **multi-rule global `security.roles` are silently dropped** (only the first rule maps into the generated config — verified live) — the deployed manifest uses a single global rule with `resources: [kafkas, '*']` and carries the Kafka-scoped rules under each cluster entry. Log in as `kafka-admin` / `openshift` for full console administration ([Step 2.3](02-keycloak.md#23--tls-certificate-and-keycloak-deployment)).

> ⚠️ **The Kafka connection's client secret cannot live in `kafkaClusters.properties.values[].valueFrom`** — that field is **not in the Console CRD schema**, so Kubernetes silently prunes it and the property resolves to `null` ("Invalid value null for configuration `sasl.oauthbearer.client.credentials.client.secret`" → "Failed to create new KafkaAdminClient" on every UI page, verified live). Reference Secrets via **`valuesFrom.secretRef`** instead — every key in the Secret becomes a Kafka client property, so the Secret's key must be the **full property name**:
>
> ```bash
> oc create secret generic kafka-console-kafka-oauth \
>   --from-literal=sasl.oauthbearer.client.credentials.client.secret=kafka-console-kafka-secret \
>   -n kafka-console
> ```
> ```yaml
> kafkaClusters:
>   - properties:
>       valuesFrom:
>         - secretRef:
>             name: kafka-console-kafka-oauth   # key = sasl.oauthbearer.client.credentials.client.secret
> ```

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
echo "Username:     kafka-admin"
echo "Password:     openshift"
```

- **URL** — the console's Route hostname, `spec.hostname` = `kafka-console.${OPENSHIFT_APPS_DOMAIN}` (verify against the live Route with `oc get routes -n kafka-console`).
- **User / password** — the `kafka-admin` user (`openshift`) for console administration, from the Keycloak realm import ([`deploy/keycloak/04-kafka-realm-import.yaml`](../deploy/keycloak/04-kafka-realm-import.yaml)); both are mapped to the `administrators` role by the `security.oidc.subjects` block above, which grants `ALL` on the virtual cluster. **Dev credentials only** — the password is committed here by design; manage real users through your identity source and never reuse these values outside a sandbox.

> Group-based authorization (mapping a JWT claim such as `groups` to roles via `subjects[].claim`) requires group membership claims in the Keycloak tokens — configure the realm accordingly, or map individual users as shown. See the console guide's [OIDC security section](https://docs.redhat.com/en/documentation/red_hat_streams_for_apache_kafka/3.2/html/using_the_streams_for_apache_kafka_console/assembly-deploying-str#ref-authentication-options-str) for the full `roles`/`subjects` syntax, including per-cluster role rules.

## 8.4 — Operational notes

- **DMZ coherence with a deliberate exception:** the console connects to the proxy's virtual clusters **and directly to the physical clusters' TLS listener** (9093) — the [Step 3.4](03-kafka-cluster.md#34--restrict-kafka-ingress-to-the-proxy-namespace-dmz) lockdown allows `kafka-console` → the `tls` listener only (see the `spec.kafkaClusters[]` note above); the unauthenticated `plain` listener stays proxy/mm2-only. The console sees the *virtual* cluster (with a **Virtual** label in the selector), so topics and records you browse are exactly what proxy clients see.
- **OIDC-secured console:** the console UI is protected by `security.oidc` (Step 8.3) — browser users log in through Keycloak with the printed sandbox credentials (`kafka-admin` / `openshift`; the read-along `demo` user was removed — the only remaining console login is `kafka-admin`, see Console login above). The console's Kafka connection to the virtual cluster also authenticates with OAUTHBEARER ([Step 2.5](02-keycloak.md#25--client-authentication-at-the-proxy)) — tokens from the `kafka-console-kafka` service-account client, via the `kafkaClusters.properties` SASL settings in 13.2.
- **Namespace layout:** the console sits in the **secure backend** (only reachable in-cluster — not exposed off-cluster like the DMZ entry points) alongside the other backend namespaces — `kafka` (brokers, locked down) ← `kafka-proxy` (proxy, the DMZ door) → `kafka-http-bridge` (HTTP API, DMZ) and `kafka-console` (UI, secure backend), with `kafka-auth` (Keycloak, DMZ-resident — it issues the tokens the DMZ workloads fetch) providing the console's identity layer and `kafka-registry` (Apicurio Registry, [Step 5](05-apicurio-registry.md#step-5-deploy-apicurio-registry-and-validate-records-record-validation)) backing the proxy's Record Validation filter.
