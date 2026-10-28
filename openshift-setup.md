# Setting Up Streams for Apache Kafka Proxy on OpenShift (oc CLI)

Step-by-step instructions for deploying the full stack from the command line using the `oc` client — a **Streams for Apache Kafka cluster**, the **Red Hat build of Keycloak** OIDC authorization server (`kafka-auth` namespace), the **proxy Operator**, a working proxy with a virtual cluster, and the **Kafka HTTP Bridge** as a proxy client — based on the [Streams for Apache Kafka 3.2 proxy documentation](https://docs.redhat.com/en/documentation/red_hat_streams_for_apache_kafka/3.2/html/deploying_and_managing_streams_for_apache_kafka_proxy_on_openshift).

---

## Table of Contents

- [Prerequisites](#prerequisites)
- [Step 0: Deploy the Kafka Cluster (Streams for Apache Kafka)](#step-0-deploy-the-kafka-cluster-streams-for-apache-kafka)
- [Step 1: Install Red Hat build of Keycloak (kafka-auth namespace)](#step-1-install-red-hat-build-of-keycloak-kafka-auth-namespace)
- [Step 2: Install the Proxy Operator](#step-2-install-the-proxy-operator)
- [Step 3: Understand the Custom Resources](#step-3-understand-the-custom-resources)
- [Step 4: Create the KafkaProxy Resource](#step-4-create-the-kafkaproxy-resource)
- [Step 5: Choose and Create an Ingress](#step-5-choose-and-create-an-ingress)
- [Step 6: Define the Backend Cluster (KafkaService)](#step-6-define-the-backend-cluster-kafkaservice)
- [Step 7: (Optional) Add a Filter](#step-7-optional-add-a-filter)
- [Step 8: Create the VirtualKafkaCluster](#step-8-create-the-virtualkafkacluster)
- [Step 9: TLS Certificates](#step-9-tls-certificates)
- [Step 10: DNS for Off-Cluster Ingress](#step-10-dns-for-off-cluster-ingress)
- [Step 11: Apply and Verify](#step-11-apply-and-verify)
- [Step 12: Deploy the Kafka HTTP Bridge (DMZ Client)](#step-12-deploy-the-kafka-http-bridge-dmz-client)
- [Troubleshooting Tips](#troubleshooting-tips)

---

## Prerequisites

- **OpenShift cluster 4.18 or later** (3.2 is tested on 4.16–4.21, excluding 4.17).
- `oc` CLI installed and configured to connect to the cluster:

  ```bash
  oc login --token=<token> --server=<cluster-url>
  oc whoami
  ```

- An account with permission to create `CustomResourceDefinition` and RBAC (`ClusterRole`) resources (for the operator install):

  ```bash
  oc auth can-i create customresourcedefinitions
  oc auth can-i create clusterroles
  ```

- A **Kafka cluster available to be proxied** — it may run in the same OpenShift cluster or elsewhere; only network connectivity is required. If you don't have one, deploy one with Streams for Apache Kafka in [Step 0](#step-0-deploy-the-kafka-cluster-streams-for-apache-kafka).
- TLS certificate generation capability (for ingress types that require TLS).
- DNS management access (only for `loadBalancer` ingress).

All commands below assume a namespace of `kafka-proxy` — adjust to yours, and create it first if needed:

```bash
oc new-project kafka-proxy
```

## Step 0: Deploy the Kafka Cluster (Streams for Apache Kafka)

If you don't already have a Kafka cluster, deploy one using **Streams for Apache Kafka** (Strimzi) from the same `streams-3.2-ocp-install-examples.zip` archive. The example below deploys a small **development** cluster; production notes follow.

### 0.1 — Install the Cluster Operator

1. The Cluster Operator manifests are in `install/cluster-operator/`. Choose a namespace for the Kafka cluster (example: `kafka`) and edit the manifests to match:

   ```bash
   oc new-project kafka

   # Linux
   sed -i 's/namespace: .*/namespace: kafka/' install/cluster-operator/*.yaml

   # macOS
   sed -i '' 's/namespace: .*/namespace: kafka/' install/cluster-operator/*.yaml
   ```

   > The Cluster Operator watches the namespace set in the `STRIMZI_NAMESPACE` environment variable of its `Deployment` (defaults to the installation namespace in the manifests). To watch other namespaces, set `STRIMZI_NAMESPACE` to a comma-separated list or use `STRIMZI_NAMESPACE="*"` for all namespaces.

2. Install the Cluster Operator and wait for it to be available:

   ```bash
   oc create -f install/cluster-operator -n kafka

   oc wait --for=condition=Available deployment/strimzi-cluster-operator \
     -n kafka --timeout=180s
   ```

### 0.2 — Create the Kafka cluster

Kafka runs exclusively in **KRaft mode** (no ZooKeeper): you define the cluster with a `Kafka` resource plus `KafkaNodePool` resources. Nodes take the role of **broker**, **controller**, or both (dual-role). This dev example uses a single dual-role node pool with ephemeral storage:

```yaml
# kafka-nodepool.yaml
kind: KafkaNodePool
apiVersion: kafka.strimzi.io/v1
metadata:
  name: dual-role
  namespace: kafka
  labels:
    strimzi.io/cluster: my-cluster
spec:
  replicas: 3
  roles:
    - controller
    - broker
  storage:
    type: ephemeral
```

```yaml
# kafka.yaml
kind: Kafka
apiVersion: kafka.strimzi.io/v1
metadata:
  name: my-cluster
  namespace: kafka
  annotations:
    strimzi.io/node-pools: enabled
spec:
  kafka:
    replicas: 3
    listeners:
      - name: plain
        port: 9092
        type: internal
        tls: false
      - name: tls
        port: 9093
        type: internal
        tls: true
    config:
      offsets.topic.replication.factor: 3
      transaction.state.log.replication.factor: 3
      transaction.state.log.min.isr: 2
      default.replication.factor: 3
      min.insync.replicas: 2
    storage:
      type: ephemeral
```

> In the `v1` API, `.spec.replicas` is required on the `Kafka` resource, and node resources (CPU/memory) are managed through `KafkaNodePool`. Streams for Apache Kafka 3.2 uses **static controller quorums** — controller node pools cannot be scaled after creation.

Apply and wait for readiness:

```bash
oc apply -f kafka-nodepool.yaml -f kafka.yaml -n kafka

oc wait --for=condition=Ready kafka/my-cluster -n kafka --timeout=600s

oc get pods -n kafka
oc get services -n kafka
```

You should see broker pods (`my-cluster-dual-role-0/1/2`) and the bootstrap Service `my-cluster-kafka-bootstrap`.

### 0.3 — Note the connection details for the proxy

This cluster's bootstrap address and broker IDs feed directly into the proxy's `KafkaService` (Step 6):

- `bootstrapServers`: `my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092` (plain listener)
- Broker node IDs: `0`–`2` → `nodeIdRanges: { start: 0, end: 2 }`

Because the cluster uses the `tls` listener, the proxy can connect over TLS with the Strimzi-generated CA — `trustAnchorRef` on the `KafkaService` can auto-trust it (see Step 6).

> **Production notes:** use **dedicated controller and broker node pools** (at least 3 controllers), `persistent-claim` storage sized for your retention, the `tls` listener with authentication (`tls` client auth or OAuth), and consider Cruise Control for rebalancing. See [*Deploying and Managing Streams for Apache Kafka on OpenShift*](https://docs.redhat.com/en/documentation/red_hat_streams_for_apache_kafka/3.2/html/deploying_and_managing_streams_for_apache_kafka_on_openshift) for full configuration guidance.

## Step 1: Install Red Hat build of Keycloak (kafka-auth namespace)

Deploy the **Red Hat build of Keycloak** as the OIDC authorization server for the platform — clients will obtain JWTs from it and authenticate with `SASL/OAUTHBEARER` against the Kafka cluster's OAuth listener. Based on the [RHBK 26.4 Operator Guide](https://docs.redhat.com/en/documentation/red_hat_build_of_keycloak/26.4/html/operator_guide) (check [access.redhat.com](https://access.redhat.com/support/policy/updates/red_hat_build_of_keycloak_notes) for the latest supported release).

### 1.1 — Install the Keycloak Operator into `kafka-auth`

The operator is installed via OLM. Namespace, OperatorGroup, and Subscription are consolidated into a single file applied with one `oc apply`:

```yaml
# kafka-auth-operator.yaml
---
apiVersion: v1
kind: Namespace
metadata:
  name: kafka-auth
---
apiVersion: operators.coreos.com/v1
kind: OperatorGroup
metadata:
  name: keycloak-operator-group
  namespace: kafka-auth
spec: {}
---
apiVersion: operators.coreos.com/v1alpha1
kind: Subscription
metadata:
  name: rhbk-operator
  namespace: kafka-auth
spec:
  channel: <desired-channel>          # channel matching the RHBK version you want
  name: rhbk-operator
  source: redhat-operators
  sourceNamespace: openshift-marketplace
  installPlanApproval: Manual         # strongly recommended — see note below
```

> ⚠️ **Use `Manual` approval.** By default OLM auto-upgrades the operator, which pulls in unintended RHBK server upgrades and can require DB migration — with no downgrade path. With `Manual`, approve upgrades deliberately after reviewing release notes and backing up the database.

Apply everything in one step and wait for the operator:

```bash
oc apply -f kafka-auth-operator.yaml

oc get subscriptions -n kafka-auth
oc wait --for=jsonpath='{.status.phase}'=Succeeded csv --all -n kafka-auth --timeout=300s
oc get pods -n kafka-auth
```

> `oc apply -f <dir-or-file>` creates the namespace, the OperatorGroup, and subscribes in a single transaction — note that `Namespace`/`OperatorGroup`/`Subscription` resources must be applied together (as in this file) so the OperatorGroup exists when the Subscription is validated.

### 1.2 — Provision a database (dev: ephemeral PostgreSQL)

The operator does **not** manage the database. For development, deploy an ephemeral PostgreSQL:

```yaml
# example-postgres.yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: postgresql-db
spec:
  serviceName: postgresql-db-service
  selector:
    matchLabels:
      app: postgresql-db
  replicas: 1
  template:
    metadata:
      labels:
        app: postgresql-db
    spec:
      containers:
        - name: postgresql-db
          image: postgres:15
          volumeMounts:
            - mountPath: /data
              name: cache-volume
          env:
            - name: POSTGRES_USER
              value: testuser
            - name: POSTGRES_PASSWORD
              value: testpassword
            - name: PGDATA
              value: /data/pgdata
            - name: POSTGRES_DB
              value: keycloak
      volumes:
        - name: cache-volume
          emptyDir: {}
---
apiVersion: v1
kind: Service
metadata:
  name: postgres-db
spec:
  selector:
    app: postgresql-db
    ports:
      - port: 5432
        targetPort: 5432
```

```bash
oc apply -f example-postgres.yaml -n kafka-auth

# Store the DB credentials in a Secret
oc create secret generic keycloak-db-secret \
  --from-literal=username=testuser \
  --from-literal=password=testpassword \
  -n kafka-auth
```

> **Production:** provision a supported database with persistent storage (cloud offering or a database operator) — not the ephemeral dev setup above.

### 1.3 — TLS certificate and Keycloak deployment

For development, generate a self-signed certificate and deploy Keycloak with an OpenShift-assigned hostname (`ingress.className: openshift-default` lets the operator assign one):

```bash
openssl req -subj '/CN=kafka-auth/O=Kafka Auth./C=US' \
  -newkey rsa:2048 -nodes -keyout key.pem -x509 -days 365 -out certificate.pem

oc create secret tls example-tls-secret --cert certificate.pem --key key.pem -n kafka-auth
```

```yaml
# example-kc.yaml
apiVersion: k8s.keycloak.org/v2alpha1
kind: Keycloak
metadata:
  name: example-kc
  namespace: kafka-auth
spec:
  instances: 1
  db:
    vendor: postgres
    host: postgres-db
    usernameSecret:
      name: keycloak-db-secret
      key: username
    passwordSecret:
      name: keycloak-db-secret
      key: password
  http:
    tlsSecret: example-tls-secret
  hostname:
    hostname: kafka-auth.example.com   # or leave unpopulated with the openshift-default IngressClass
  ingress:
    className: openshift-default
  proxy:
    headers: xforwarded
```

Apply and wait for readiness:

```bash
oc apply -f example-kc.yaml -n kafka-auth

oc wait --for=condition=Ready keycloak/example-kc -n kafka-auth --timeout=600s
oc get keycloaks/example-kc -n kafka-auth \
  -o go-template='{{range .status.conditions}}CONDITION: {{.type}}  STATUS: {{.status}}{{"\n"}}{{end}}'
```

Fetch the initial admin credentials (the operator stores them in a Secret):

```bash
oc get secret example-kc-initial-admin -o jsonpath='{.data.username}' | base64 --decode
oc get secret example-kc-initial-admin -o jsonpath='{.data.password}' | base64 --decode
```

> **Production:** change the initial admin credentials, enable MFA, and use a real hostname with a CA-signed certificate. Restrict who can create/edit `Keycloak` and `KeycloakRealmImport` CRs — they effectively hold namespace-level privileges.

### 1.4 — Create the `kafka` realm and clients

Automate realm creation with a `KeycloakRealmImport` CR. The import creates the `kafka` realm with **all the clients the platform needs**: the broker-side client used by the Kafka listener, service-account clients for producing and consuming workloads, the client the HTTP Bridge uses for Kafka-side authentication, and a client for the API gateway in front of the bridge:

```yaml
# kafka-realm-import.yaml
apiVersion: k8s.keycloak.org/v2alpha1
kind: KeycloakRealmImport
metadata:
  name: kafka-realm-import
  namespace: kafka-auth
spec:
  keycloakCRName: example-kc
  realm:
    realm: kafka
    enabled: true
    # Short-lived access tokens (5 min) — access stops quickly on revocation
    accessTokenLifespan: 300
    clients:
      # 1. Broker client — used by the Kafka listener (Step 1.5) for token
      #    validation via the introspection endpoint (or authenticated JWKS)
      - clientId: kafka-broker
        enabled: true
        protocol: openid-connect
        publicClient: false
        secret: kafka-broker-secret
        serviceAccountsEnabled: true
        standardFlowEnabled: false
      # 2. Producer client — SASL/OAUTHBEARER credentials for producing clients
      #    (client id/secret = the "username/password" in the login module)
      - clientId: kafka-producer
        enabled: true
        protocol: openid-connect
        publicClient: false
        secret: kafka-producer-secret
        serviceAccountsEnabled: true
        standardFlowEnabled: false
      # 3. Consumer client — SASL/OAUTHBEARER credentials for consuming clients
      - clientId: kafka-consumer
        enabled: true
        protocol: openid-connect
        publicClient: false
        secret: kafka-consumer-secret
        serviceAccountsEnabled: true
        standardFlowEnabled: false
      # 4. HTTP Bridge client — used by the bridge (Step 12) to authenticate to
      #    the Kafka listener over SASL/OAUTHBEARER via `custom` authentication
      - clientId: kafka-bridge
        enabled: true
        protocol: openid-connect
        publicClient: false
        secret: kafka-bridge-secret
        serviceAccountsEnabled: true
        standardFlowEnabled: false
      # 5. API gateway client — for the gateway in front of the bridge (Step 12.5);
      #    obtains access tokens for HTTP clients via the client-credentials grant
      - clientId: kafka-api-gateway
        enabled: true
        protocol: openid-connect
        publicClient: false
        secret: kafka-api-gateway-secret
        serviceAccountsEnabled: true
        standardFlowEnabled: false
```

Where each client is used:

| Client | Used by | Where |
|---|---|---|
| `kafka-broker` | Kafka listener (token validation) | Step 1.5 |
| `kafka-producer` | Producing clients (SASL/OAUTHBEARER) | Step 1.5 |
| `kafka-consumer` | Consuming clients (SASL/OAUTHBEARER) | Step 1.5 |
| `kafka-bridge` | HTTP Bridge → Kafka listener auth | Step 12.2 |
| `kafka-api-gateway` | Gateway in front of the bridge (client-credentials) | Step 12.5 |

```bash
oc apply -f kafka-realm-import.yaml -n kafka-auth
oc wait --for=condition=Done keycloakrealmimport/kafka-realm-import -n kafka-auth --timeout=300s
```

Verify the OIDC endpoints are reachable, and obtain a token as a service account (client-credentials grant — the same flow the Kafka clients use):

```bash
oc get routes -n kafka-auth
# Token endpoint: https://<keycloak-hostname>/realms/kafka/protocol/openid-connect/token

curl -s https://<keycloak-hostname>/realms/kafka/protocol/openid-connect/token \
  -d grant_type=client_credentials \
  -d client_id=kafka-producer \
  -d client_secret=kafka-producer-secret | jq -r .access_token
```

> Each client's `secret` above is defined inline for simplicity. In production, store client secrets in Keycloak (rotated there) or in a Secret-backed vault rather than committing them in YAML — and keep the `kafka-auth` namespace access tightly restricted.

### 1.5 — Wire Keycloak to the Kafka listener

The Kafka cluster (Step 0) authenticates clients via OAuth against Keycloak. Replace/add an `oauth` listener on the `Kafka` resource:

```yaml
kind: Kafka
# ...
spec:
  kafka:
    listeners:
      - name: oauth
        port: 9094
        type: internal
        tls: true
        authentication:
          type: oauth
          validIssuerUri: https://kafka-auth.example.com/realms/kafka
          jwksEndpointUri: https://kafka-auth.example.com/realms/kafka/protocol/openid-connect/certs
          jwksRefreshSeconds: 300
          jwksExpirySeconds: 600
          userNameClaim: preferred_username
          clientId: kafka-broker
          # introspection alternative:
          # introspectionEndpointUri: https://kafka-auth.example.com/realms/kafka/protocol/openid-connect/token/introspect
          clientSecret:
            secretName: kafka-broker-secret
            key: kafka-broker-secret
```

Clients then authenticate with `SASL/OAUTHBEARER`, passing `org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule` with the token from Keycloak's token endpoint as the password (`clientId`/`secret` as username/password in the login module). The **proxy passes the authenticated SASL exchange through** to this listener, so end-user identity reaches the broker and Kafka ACLs apply — see use case [5 in the guide](README.md#5-oidc--oauth-20-authentication-with-red-hat-build-of-keycloak).

> Use **HTTPS issuer/JWKS URIs** trusted by the brokers (private CA → add the CA to the broker truststore or reference a truststore via `oauth.validIssuerUri` TLS config). See the Streams for Apache Kafka [*Configuring OAuth 2.0 token-based security*](https://docs.redhat.com/en/documentation/red_hat_streams_for_apache_kafka/3.2/html/deploying_and_managing_streams_for_apache_kafka_on_openshift) guide for full listener options.

### 0.4 — Restrict Kafka ingress to the proxy namespace (DMZ)

Because the goal is to run the proxy in a **DMZ**, lock down the Kafka cluster's network surface: the only namespace allowed to reach the Kafka listeners is `kafka-proxy`. Kubernetes `NetworkPolicy` rules are additive, so combine three policies on the Kafka pods (selected by the Strimzi label `strimzi.io/cluster: my-cluster`):

1. **Default deny** — block all ingress to Kafka pods.
2. **Same-namespace** — allow all traffic from within the `kafka` namespace (controller↔broker on the control-plane port, broker↔broker replication, Cluster/Entity Operator, Cruise Control).
3. **Proxy-only clients** — allow ingress from the `kafka-proxy` namespace **only** on the client-facing listener ports (9092 plain, 9093 TLS, 9094 OAuth).

```yaml
# kafka-netpol-dmz.yaml
---
# 1. Default deny: no ingress to Kafka pods at all
kind: NetworkPolicy
apiVersion: networking.k8s.io/v1
metadata:
  name: my-cluster-deny-ingress
  namespace: kafka
spec:
  podSelector:
    matchLabels:
      strimzi.io/cluster: my-cluster
  policyTypes:
    - Ingress
  ingress: []
---
# 2. Allow all traffic from within the kafka namespace
#    (control plane 9091, broker replication, Cluster/Entity Operator)
kind: NetworkPolicy
apiVersion: networking.k8s.io/v1
metadata:
  name: my-cluster-allow-same-namespace
  namespace: kafka
spec:
  podSelector:
    matchLabels:
      strimzi.io/cluster: my-cluster
  ingress:
    - from:
        - podSelector: {}   # any pod in the kafka namespace
---
# 3. DMZ ingress: only the kafka-proxy namespace, only on client-facing ports
kind: NetworkPolicy
apiVersion: networking.k8s.io/v1
metadata:
  name: my-cluster-allow-proxy
  namespace: kafka
spec:
  podSelector:
    matchLabels:
      strimzi.io/cluster: my-cluster
  ingress:
    - from:
        - namespaceSelector:
            matchLabels:
              kubernetes.io/metadata.name: kafka-proxy
      ports:
        - port: 9092
          protocol: TCP
        - port: 9093
          protocol: TCP
        - port: 9094
          protocol: TCP
```

Apply and verify:

```bash
oc apply -f kafka-netpol-dmz.yaml -n kafka
oc get networkpolicies -n kafka
```

Verify the DMZ behaves as intended:

```bash
# From another namespace (e.g., default): connection should TIME OUT
oc run kcat-blocked --rm -i --restart=Never --image=confluentinc/cp-kcat:latest -n default -- \
  -b my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092 -L

# From the kafka-proxy namespace: connection succeeds (this is what the proxy uses)
oc run kcat-allowed --rm -i --restart=Never --image=confluentinc/cp-kcat:latest -n kafka-proxy -- \
  -b my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092 -L
```

Notes:

- **Cluster Operator in another namespace?** The same-namespace rule covers the operator because this guide installs it into `kafka`. If yours runs elsewhere, add a `namespaceSelector` entry for its namespace to policy 2.
- **Direct-broker checks**: with the DMZ in place, "consume directly from the broker" verification (e.g., checking ciphertext after enabling Record Encryption) must run from the `kafka` or `kafka-proxy` namespace — from any other namespace it will be denied. That denial is the point.
- **Do not** add a `kubernetes.io/metadata.name: kafka` label requirement to policy 2 — podSelector rules already match by namespace; keep the same-namespace rule as shown.
- For defense in depth, also restrict the `kafka-proxy` namespace's **egress** to the Kafka brokers (plus the operator/API servers) so the proxy pod cannot be used as a pivot into the cluster.

## Step 2: Install the Proxy Operator

### Recommended: installation files

1. Download the release archive `streams-3.2-ocp-install-examples.zip` from the [Streams for Apache Kafka software downloads page](https://access.redhat.com/jbossnetwork/restricted/listSoftware.html?downloadType=distributions&product=jboss.amq.streams) and extract it:

   ```bash
   unzip streams-3.2-ocp-install-examples.zip -d streams-kafka-proxy
   cd streams-kafka-proxy
   ```

   - `install/` contains the YAML manifests for the operator.
   - `examples/` contains example custom resources for deploying a proxy.

2. Choose the namespace the operator will run in (example: `kafka-proxy`) and edit the installation files to match:

   ```bash
   oc new-project kafka-proxy

   # Linux
   sed -i 's/namespace: .*/namespace: kafka-proxy/' install/*.yaml

   # macOS
   sed -i '' 's/namespace: .*/namespace: kafka-proxy/' install/*.yaml
   ```

3. Deploy the operator (creates the CRDs, RBAC, and operator deployment):

   ```bash
   oc create -f install
   ```

4. Verify the deployment:

   ```bash
   oc wait --for=condition=Available deployment/kroxylicious-operator \
     -n kafka-proxy --timeout=120s

   oc get deployments -n kafka-proxy
   ```

   Expected output:

   ```text
   NAME                      READY  UP-TO-DATE  AVAILABLE
   kroxylicious-operator     1/1    1           1
   ```

### Alternative: OperatorHub (consolidated YAML)

If you prefer an OLM-managed install, consolidate Namespace, OperatorGroup, and Subscription into one file and apply it with `oc apply`. First discover the exact package name:

```bash
oc get packagemanifests -n openshift-marketplace | grep -i "proxy"
```

```yaml
# kafka-proxy-operator-subscription.yaml
---
apiVersion: v1
kind: Namespace
metadata:
  name: kafka-proxy
---
apiVersion: operators.coreos.com/v1
kind: OperatorGroup
metadata:
  name: proxy-operator-group
  namespace: kafka-proxy
spec: {}   # global OperatorGroup — the operator watches and manages proxies cluster-wide
---
apiVersion: operators.coreos.com/v1alpha1
kind: Subscription
metadata:
  name: streams-kafka-proxy-operator
  namespace: kafka-proxy
spec:
  channel: alpha                # or amq-streams-X.Y.x for version-pinned updates
  name: <package-name>          # from the packagemanifests output above
  source: redhat-operators
  sourceNamespace: openshift-marketplace
  installPlanApproval: Manual   # auto-upgrades on the default alpha channel skip pre-upgrade steps
```

```bash
oc apply -f kafka-proxy-operator-subscription.yaml

oc get subscriptions -n kafka-proxy
oc wait --for=jsonpath='{.status.phase}'=Succeeded csv --all -n kafka-proxy --timeout=300s
```

The rest of this guide works identically regardless of install method.

## Step 3: Understand the Custom Resources

The operator consumes these custom resources (API version `kroxylicious.io/v1alpha1`) and generates the OpenShift `Deployment`, `Service`, and `ConfigMap` resources that run the proxy:

| Resource | Purpose |
|---|---|
| `KafkaProxy` | An instance of the proxy; the top-level resource linking everything together |
| `KafkaProxyIngress` | How a virtual cluster is exposed on the network |
| `KafkaService` | The backend (upstream) Kafka cluster |
| `VirtualKafkaCluster` | The logical cluster presented to clients |
| `KafkaProtocolFilter` | Filter applied to traffic on a virtual cluster |
| `Secret` / `ConfigMap` | Referenced by `KafkaService` / `KafkaProtocolFilter` for TLS certs, tokens, and CA material |

> The API is deliberately decomposed (similar to the Kubernetes Gateway API), so you can use RBAC to split responsibility — e.g., network teams own `KafkaProxy`/`KafkaProxyIngress`, application teams own `VirtualKafkaCluster`/`KafkaService`/`KafkaProtocolFilter`.

## Step 4: Create the KafkaProxy Resource

An empty `spec` creates a proxy with default configuration:

```yaml
kind: KafkaProxy
apiVersion: kroxylicious.io/v1alpha1
metadata:
  namespace: kafka-proxy
  name: simple
spec: {}
```

## Step 5: Choose and Create an Ingress

A `KafkaProxyIngress` defines how clients reach the virtual cluster. It belongs to one `KafkaProxy` (via `proxyRef`) but can be shared by multiple virtual clusters.

| Ingress Type | Use case | Transport | DNS changes needed? |
|---|---|---|---|
| `clusterIP` | On-cluster applications | TCP / TLS | No |
| `loadBalancer` | Off-cluster applications | TLS only | Yes |
| `openShiftRoute` | Off-cluster applications (OpenShift only) | TLS only | No — hostnames assigned by OpenShift |

**clusterIP (TCP or TLS)** — uses a `ClusterIP` Service; for on-cluster clients:

```yaml
kind: KafkaProxyIngress
apiVersion: kroxylicious.io/v1alpha1
metadata:
  namespace: kafka-proxy
  name: cluster-ip
spec:
  proxyRef:
    name: simple
  clusterIP:
    protocol: TCP        # or TLS
```

**loadBalancer (TLS only)** — uses a `LoadBalancer` Service; the proxy uses SNI to route, so every bootstrap and broker address must be unique in DNS. The `$(nodeId)` token is required in the broker pattern; `$(virtualClusterName)` may be used when sharing the ingress across virtual clusters:

```yaml
kind: KafkaProxyIngress
apiVersion: kroxylicious.io/v1alpha1
metadata:
  namespace: kafka-proxy
  name: load-balancer
spec:
  proxyRef:
    name: simple
  loadBalancer:
    bootstrapAddress: "$(virtualClusterName).kafkaproxy.example.com"
    advertisedBrokerAddressPattern: "broker-$(nodeId).$(virtualClusterName).kafkaproxy.example.com"
```

**openShiftRoute (TLS only)** — uses OpenShift `Route` resources; bootstrap and broker hostnames are generated automatically as `<virtualClusterName>-bootstrap.<ingressDomain>:443` and `<virtualClusterName>-<nodeId>.<ingressDomain>:443`:

```yaml
kind: KafkaProxyIngress
apiVersion: kroxylicious.io/v1alpha1
metadata:
  namespace: kafka-proxy
  name: openshift-route
spec:
  proxyRef:
    name: simple
  openShiftRoute: {}
```

> TLS is recommended for production connections. `loadBalancer` and `openShiftRoute` support TLS only.

## Step 6: Define the Backend Cluster (KafkaService)

The `KafkaService` describes the upstream Kafka cluster. Declare the full range of broker node IDs:

```yaml
kind: KafkaService
apiVersion: kroxylicious.io/v1alpha1
metadata:
  namespace: kafka-proxy
  name: my-cluster
spec:
  bootstrapServers: my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092  # the cluster from Step 0
  nodeIdRanges:
    - name: brokers
      start: 0
      end: 2
```

- `bootstrapServers` specifies a comma-separated list of addresses in `<host>:<port>` format. Including multiple broker addresses helps clients connect when one is unavailable. For an external cluster, use its reachable bootstrap address (e.g., `kafka.example.com:9092`).
- `nodeIdRanges` must cover **all** broker node IDs in the target cluster (`start` and `end` inclusive). For the cluster deployed in Step 0, the dual-role broker IDs are `0`–`2`; check your `Kafka`/`KafkaNodePool` resources otherwise.
- TLS to the upstream can be configured here — for a cluster using the `tls` listener, `trustAnchorRef` accepts a `Secret`, and the operator can auto-trust the Strimzi-generated CA anchor.

## Step 7: (Optional) Add a Filter

A `KafkaProtocolFilter` is reusable across virtual clusters in the same namespace. `spec.type` names the filter implementation and `spec.configTemplate` carries the configuration (subject to operator interpolation):

```yaml
kind: KafkaProtocolFilter
apiVersion: kroxylicious.io/v1alpha1
metadata:
  namespace: kafka-proxy
  name: my-filter
spec:
  type: RecordValidation
  configTemplate:
    rules:
      - topicNames:
          - my-topic
        valueRule:
          syntacticallyCorrectJson:
            allowNulls: false
            allowEmpty: false
```

Start with **no filters** to validate basic connectivity, then add one at a time. See the [use case catalog](README.md#use-case-catalog) for Record Encryption and Record Validation configuration.

## Step 8: Create the VirtualKafkaCluster

The `VirtualKafkaCluster` wires everything together. All referenced resources must be in the same namespace. Filter order matters: client requests pass through filters from index `0` to `n`; broker responses pass through in reverse order. An empty/omitted `filterRefs` passes traffic through unchanged.

Minimal example (single ingress, one filter, on-cluster clients):

```yaml
kind: VirtualKafkaCluster
apiVersion: kroxylicious.io/v1alpha1
metadata:
  name: my-cluster
  namespace: kafka-proxy
spec:
  proxyRef:
    name: simple
  targetKafkaServiceRef:
    name: my-cluster
  ingresses:
    - ingressRef:
        name: cluster-ip
  filterRefs:
    - name: my-filter
```

TLS-enabled example with two ingresses (on- and off-cluster clients), each with its own server certificate:

```yaml
kind: VirtualKafkaCluster
apiVersion: kroxylicious.io/v1alpha1
metadata:
  name: my-cluster
  namespace: kafka-proxy
spec:
  proxyRef:
    name: simple
  targetKafkaServiceRef:
    name: my-cluster
  ingresses:
    - ingressRef:
        name: cluster-ip
      tls:
        certificateRef:
          name: cluster-ip-server-cert
          kind: Secret
    - ingressRef:
        name: load-balancer
      tls:
        certificateRef:
          name: external-server-cert
          kind: Secret
  filterRefs:
    - name: my-filter
```

## Step 9: TLS Certificates

TLS ingress types need a server certificate provided as a `Secret` of type `kubernetes.io/tls`:

```bash
oc create secret tls <secret-name> --namespace <namespace> --cert=<path/to/cert/file> --key=<path/to/key/file>
```

Certificate requirements:

- **PKCS#8 encoded PEM** private key, not password protected.
- **SANs must cover every address** clients will use (or use a wildcard certificate).
- Signed by a CA trusted by the **connecting applications** (on-cluster CA for `clusterIP`, off-cluster/public CA for `loadBalancer` and `openShiftRoute`).

Which SANs are needed per ingress type (assuming virtual cluster `my-cluster`, brokers 0–2):

| Ingress | SANs |
|---|---|
| `clusterIP` | `my-cluster-cluster-ip-bootstrap.<namespace>.svc.cluster.local` plus `my-cluster-cluster-ip-<nodeId>.<namespace>.svc.cluster.local` for each broker |
| `loadBalancer` | `mycluster.kafkaproxy.example.com` plus `broker-<nodeId>.mycluster.kafkaproxy.example.com` (derived from your `bootstrapAddress` / `advertisedBrokerAddressPattern`) |
| `openShiftRoute` | `mycluster-bootstrap.<ingressDomain>` plus `mycluster-<nodeId>.<ingressDomain>` — look up the ingress domain first: |

```bash
oc get ingresscontrollers.operator.openshift.io -n openshift-ingress-operator default -o=jsonpath='{.status.domain}'
```

> Ingress Controller sharding can result in different ingress domains per address — include all of them as SANs.
>
> Concretely, for this guide's `kafka-proxy` namespace with brokers 0–2, the `clusterIP` SANs are:
>
> ```text
> my-cluster-cluster-ip-bootstrap.kafka-proxy.svc.cluster.local
> my-cluster-cluster-ip-0.kafka-proxy.svc.cluster.local
> my-cluster-cluster-ip-1.kafka-proxy.svc.cluster.local
> my-cluster-cluster-ip-2.kafka-proxy.svc.cluster.local
> ```

## Step 10: DNS for Off-Cluster Ingress

**`openShiftRoute`**: nothing to do — OpenShift assigns the hostnames.

**`loadBalancer`**: bootstrap and broker names must resolve to the load balancer's external address:

1. Discover the external address:

   ```bash
   oc get virtualkafkacluster -n <namespace> <virtual-cluster-name> \
     -o=jsonpath='{.status.ingresses[?(@.name == "<ingress-name>")].loadBalancerIngressPoints}'
   ```

2. Create DNS mappings (example for `my-cluster` with brokers 0–2):

   ```text
   my-cluster.kafkaproxy.example.com            => <external address>
   broker-0.my-cluster.kafkaproxy.example.com   => <external address>
   broker-1.my-cluster.kafkaproxy.example.com   => <external address>
   broker-2.my-cluster.kafkaproxy.example.com   => <external address>
   ```

3. Verify resolution from the application network:

   ```bash
   nslookup my-cluster.kafkaproxy.example.com
   nslookup broker-0.my-cluster.kafkaproxy.example.com
   ```

## Step 11: Apply and Verify

1. Apply the resources (order doesn't strictly matter — the operator reconciles references):

   ```bash
   oc apply -f kafkaproxy.yaml -f ingress.yaml -f kafkaservice.yaml -f virtualcluster.yaml
   ```

2. **Wait for the operator to reconcile the resources.** The operator reports status conditions on the custom resources:

   - `ResolvedRefs=True` — all referenced resources exist (`KafkaProxy`, `KafkaProxyIngress`, `Secret`, ...)
   - `Accepted=True` — the configuration was successfully applied to the proxy

   ```bash
   oc wait --for=condition=ResolvedRefs=True virtualkafkacluster/my-cluster \
     -n kafka-proxy --timeout=120s

   oc wait --for=condition=Accepted=True virtualkafkacluster/my-cluster \
     -n kafka-proxy --timeout=120s

   oc wait --for=condition=ResolvedRefs=True kafkaproxyingress/cluster-ip \
     -n kafka-proxy --timeout=120s
   ```

   If these time out, inspect the conditions for `reason` and `message` details:

   ```bash
   oc get virtualkafkacluster my-cluster -n kafka-proxy \
     -o jsonpath='{.status.conditions}' | jq
   ```

3. **Wait for the proxy pods to be ready.** The operator creates a `Deployment` named after the `KafkaProxy` resource (here: `simple`):

   ```bash
   oc wait --for=condition=Available deployment/simple -n kafka-proxy --timeout=120s
   oc get pods -n kafka-proxy
   ```

4. Connect a client using the bootstrap address for your ingress type:
   - `clusterIP`: `<virtual-cluster>-<ingress>-bootstrap.<namespace>.svc.cluster.local` (check the generated Service for the exact port):

     ```bash
     oc get services -n kafka-proxy
     ```

   - `loadBalancer` / `openShiftRoute`: the bootstrap address from your DNS/Route configuration.

5. **Smoke-test end-to-end (plain TCP)** from inside the cluster with `kcat` (adjust image/bootstrap as needed):

   ```bash
   oc run kcat-producer --rm -i --restart=Never --image=confluentinc/cp-kcat:latest -- \
     -b my-cluster-cluster-ip-bootstrap.kafka-proxy.svc.cluster.local:9082 \
     -t test-topic -P -l <<< "hello through the proxy"

   oc run kcat-consumer --rm -i --restart=Never --image=confluentinc/cp-kcat:latest -- \
     -b my-cluster-cluster-ip-bootstrap.kafka-proxy.svc.cluster.local:9082 \
     -t test-topic -C -o beginning
   ```

6. **Smoke-test end-to-end (TLS variant)** — for TLS ingresses (`clusterIP` with `protocol: TLS`, `loadBalancer`, or `openShiftRoute`), the client must trust the CA that signed the proxy's server certificate. Extract the CA, make it available to the test pod via a ConfigMap, and point kcat at it:

   ```bash
   # 1. Extract the CA certificate (from the Secret/ConfigMap holding the signing CA)
   oc get secret <ca-secret> -n kafka-proxy -o jsonpath='{.data.ca\.crt}' | base64 -d > ca.crt

   # 2. Provide it to the test pod as a ConfigMap
   oc create configmap proxy-ca --from-file=ca.crt -n kafka-proxy

   # 3. Run kcat with the CA mounted and TLS enabled
   oc run kcat-producer --rm -i --restart=Never \
     --image=confluentinc/cp-kcat:latest \
     --overrides='{
       "spec": {
         "volumes": [{"name": "proxy-ca", "configMap": {"name": "proxy-ca"}}],
         "containers": [{
           "name": "kcat-producer",
           "volumeMounts": [{"name": "proxy-ca", "mountPath": "/etc/proxy-ca", "readOnly": true}]
         }]
       }
     }' -- \
     -b my-cluster-cluster-ip-bootstrap.kafka-proxy.svc.cluster.local:9083 \
     -t test-topic -P -l \
     -X security.protocol=SSL \
     -X ssl.ca.location=/etc/proxy-ca/ca.crt \
     <<< "hello through the proxy (TLS)"
   ```

   Notes for the TLS variant:

   - Connect using the **SAN name from the server certificate** (the generated bootstrap hostname) so SNI and certificate validation succeed — never an IP address.
   - `openShiftRoute` bootstrap endpoints use port **443**.
   - If the ingress uses client-certificate (mutual TLS) authentication, add `-X ssl.key.location`, `-X ssl.certificate.location`, and `-X ssl.key.password` as required.
   - `-X enable.ssl.certificate.verification=false` can be used **for quick smoke tests only** — never in production.

7. Continue with [Getting Started](README.md#getting-started-on-openshift) to add filters.

## Step 12: Deploy the Kafka HTTP Bridge (DMZ Client)

The **HTTP Bridge** exposes the Kafka cluster over a REST API (`/topics`, `/consumers`), so HTTP clients — scripts, lightweight services, systems where a Kafka client library is impractical — can produce and consume without a native Kafka protocol connection. Deployed via the Cluster Operator as a `KafkaBridge` resource, it integrates with the proxy by being a **plain Kafka client of the virtual cluster**: point its `bootstrapServers` at the proxy's clusterIP bootstrap Service so HTTP traffic flows **through the proxy**, keeping the DMZ intact.

Placing the bridge in its own **`kafka-http-bridge` namespace** keeps the HTTP API isolated from both the proxy and the brokers — external HTTP clients reach only this namespace, and the bridge→proxy→broker path stays inside the DMZ. Based on the [Using the Streams for Apache Kafka HTTP Bridge](https://docs.redhat.com/en/documentation/red_hat_streams_for_apache_kafka/3.2/html/using_the_streams_for_apache_kafka_http_bridge) guide.

### 12.1 — Create the namespace and extend the Cluster Operator watch

Create the namespace:

```bash
oc new-project kafka-http-bridge
```

The Cluster Operator (Step 0) watches only the `kafka` namespace, so it would not reconcile a `KafkaBridge` in `kafka-http-bridge`. Extend its watch scope:

```bash
# 1. Add kafka-http-bridge to STRIMZI_NAMESPACE in the operator Deployment
oc set env deployment/strimzi-cluster-operator -n kafka STRIMZI_NAMESPACE=kafka,kafka-http-bridge

# 2. Install the RoleBindings for the watched namespace
sed -i 's/namespace: .*/namespace: kafka/' install/cluster-operator/*RoleBinding*.yaml
oc apply -f install/cluster-operator/020-RoleBinding-strimzi-cluster-operator.yaml -n kafka-http-bridge
oc apply -f install/cluster-operator/023-RoleBinding-strimzi-cluster-operator.yaml -n kafka-http-bridge
oc apply -f install/cluster-operator/031-RoleBinding-strimzi-cluster-operator-entity-operator-delegation.yaml -n kafka-http-bridge

# 3. Restart the operator to pick up the new watch scope
oc rollout restart deployment/strimzi-cluster-operator -n kafka
```

> Alternatively, set `STRIMZI_NAMESPACE="*"` to watch all namespaces (see [Deploying the Cluster Operator to watch all namespaces](https://strimzi.io/docs/operators/latest/deploying.html#deploying-cluster-operator-to-watch-whole-cluster-str)).

### 12.2 — Deploy the bridge with a `KafkaBridge` resource

The bridge runs in the `kafka-http-bridge` namespace and connects to the **proxy's clusterIP bootstrap** (from Step 11 — verify the exact port with `oc get services -n kafka-proxy`):

```yaml
# kafka-bridge.yaml
apiVersion: kafka.strimzi.io/v1
kind: KafkaBridge
metadata:
  name: my-bridge
  namespace: kafka-http-bridge
spec:
  # Replicas (required)
  replicas: 1
  # Kafka bootstrap servers (required) — the PROXY's clusterIP bootstrap, not the Kafka cluster's
  bootstrapServers: my-cluster-cluster-ip-bootstrap.kafka-proxy.svc.cluster.local:9082
  # HTTP configuration (required)
  http:
    port: 8080
  # If the proxy's clusterIP ingress uses TLS, trust the CA that signed the proxy's
  # server certificate (same CA extracted in Step 11, TLS variant — create the Secret
  # in THIS namespace, not kafka-proxy):
  # tls:
  #   trustedCertificates:
  #     - secretName: proxy-ca
  #       certificate: ca.crt
  # If the Kafka listener behind the proxy uses OAuth (Step 1.5), authenticate the
  # bridge with the `kafka-bridge` client from Step 1.4 via `custom` authentication:
  # authentication:
  #   type: custom
  #   sasl: true
  #   config:
  #     sasl.mechanism: OAUTHBEARER
  #     sasl.login.callback.handler.class: io.strimzi.kafka.oauth.client.JaasClientOauthLoginCallbackHandler
  #     sasl.jaas.config: |
  #       org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required
  #         oauth.token.endpoint.uri="https://kafka-auth.example.com/realms/kafka/protocol/openid-connect/token"
  #         oauth.client.id="kafka-bridge"
  #         oauth.client.secret="${strimzienv:KAFKA_BRIDGE_CLIENT_SECRET}";
  #   template:
  #     bridgeContainer:
  #       env:
  #         - name: KAFKA_BRIDGE_CLIENT_SECRET
  #           valueFrom:
  #             secretKeyRef:
  #               name: kafka-bridge-client-secret
  #               key: client-secret
```

If using the OAuth variant, first store the `kafka-bridge` client secret (from Step 1.4) in a Secret:

```bash
oc create secret generic kafka-bridge-client-secret \
  --from-literal=client-secret=kafka-bridge-secret \
  -n kafka-http-bridge
```

Apply and wait for readiness:

```bash
oc apply -f kafka-bridge.yaml -n kafka-http-bridge

oc wait --for=condition=Ready kafkabridge/my-bridge -n kafka-http-bridge --timeout=300s
oc get pods -n kafka-http-bridge
```

Expected output:

```text
NAME                          READY  STATUS   RESTARTS
my-bridge-bridge-<pod_id>     1/1    Running  0
```

The operator creates a Service named `<name>-bridge-service` on the configured `http.port`:

```bash
oc get services my-bridge-bridge-service -n kafka-http-bridge
```

### 12.3 — Verify produce/consume over HTTP through the proxy

```bash
# Produce a JSON record through the bridge (and therefore the proxy)
oc run bridge-producer --rm -i --restart=Never --image=curlimages/curl -n kafka-http-bridge -- \
  -X POST http://my-bridge-bridge-service:8080/topics/test-topic \
  -H "Content-Type: application/vnd.kafka.json.v2+json" \
  -d '{"records":[{"value":"hello via bridge + proxy"}]}'
```

```bash
# Create a consumer, subscribe, and fetch records
oc run bridge-consumer --rm -i --restart=Never --image=curlimages/curl -n kafka-http-bridge -- \
  -X POST http://my-bridge-bridge-service:8080/consumers/bridge-consumer-group \
  -H "Content-Type: application/vnd.kafka.v2+json" \
  -d '{"name":"bridge-consumer","format":"json","auto.offset.reset":"earliest"}'

oc run bridge-consumer --rm -i --restart=Never --image=curlimages/curl -n kafka-http-bridge -- \
  -X POST http://my-bridge-bridge-service:8080/consumers/bridge-consumer-group/instances/bridge-consumer/subscription \
  -H "Content-Type: application/vnd.kafka.v2+json" \
  -d '{"topics":["test-topic"]}'

oc run bridge-consumer --rm -i --restart=Never --image=curlimages/curl -n kafka-http-bridge -- \
  -H "Accept: application/vnd.kafka.json.v2+json" \
  http://my-bridge-bridge-service:8080/consumers/bridge-consumer-group/instances/bridge-consumer/records
```

### 12.4 — Operational notes

- **Replicas and HA:** producers are stateless, so multiple bridge replicas work well for producing workloads behind a load balancer. **Consumers are NOT recommended with multiple replicas** — consumer state (subscriptions, offsets in progress) is held in-memory per instance with no failover; if the pod dies, HTTP clients must recreate the consumer. Plan consumer-facing traffic accordingly.
- **HTTP interface security is not built in:** the bridge's REST API has no authentication/authorization of its own. Restrict access with NetworkPolicies (see [12.5](#125--restrict-http-access-to-the-bridge-dmz-networkpolicy)), or front the bridge with a reverse proxy (e.g., OAuth 2.0) or API gateway.
- **Bridge → Kafka auth:** the `KafkaBridge` CR supports `spec.authentication` (`tls`, `scram-sha-256`, `scram-sha-512`, `plain`, `custom`) and `spec.tls.trustedCertificates` — the bridge authenticates to the *Kafka listener* through the proxy like any other client. With the OAuth listener (Step 1.5), use the `custom` SASL/OAUTHBEARER configuration above with the `kafka-bridge` client from Step 1.4.
- **Exposing the bridge outside the cluster:** use an OpenShift Route for the `my-bridge-bridge-service` (HTTP clients are exempt from the Kafka-protocol constraints that make Route exposure problematic for the proxy itself).
- **DMZ consistency:** with the bridge in `kafka-http-bridge`, the same trust boundary applies — the bridge is just another client of the proxy, and the Kafka namespace still only accepts connections from `kafka-proxy`. Add the NetworkPolicy from the next section to enforce which clients can reach the bridge's HTTP port.

### 12.5 — Restrict HTTP access to the bridge (DMZ NetworkPolicy)

The bridge's REST API has no authentication of its own (see the note above), so enforce access at the network layer. The bridge pods carry the Strimzi labels `strimzi.io/kind: KafkaBridge` and `strimzi.io/cluster: my-bridge`. Combine two additive policies in the `kafka-http-bridge` namespace:

1. **Default deny** — block all ingress to the bridge pods.
2. **Allowed clients** — permit ingress on the HTTP port 8080 from within the namespace (the `oc` test pods in 12.3) and from the namespace running your API layer / gateway (example: `api-gateway` — change to yours).

```yaml
# bridge-netpol-dmz.yaml
---
# 1. Default deny: no ingress to the bridge pods
kind: NetworkPolicy
apiVersion: networking.k8s.io/v1
metadata:
  name: my-bridge-deny-ingress
  namespace: kafka-http-bridge
spec:
  podSelector:
    matchLabels:
      strimzi.io/kind: KafkaBridge
      strimzi.io/cluster: my-bridge
  policyTypes:
    - Ingress
  ingress: []
---
# 2. Allowed clients: same namespace + your API layer, only on the HTTP port
kind: NetworkPolicy
apiVersion: networking.k8s.io/v1
metadata:
  name: my-bridge-allow-clients
  namespace: kafka-http-bridge
spec:
  podSelector:
    matchLabels:
      strimzi.io/kind: KafkaBridge
      strimzi.io/cluster: my-bridge
  ingress:
    - from:
        - podSelector: {}   # any pod in kafka-http-bridge (e.g., the test pods in 12.3)
        - namespaceSelector:
            matchLabels:
              kubernetes.io/metadata.name: api-gateway   # your API layer / gateway namespace
      ports:
        - port: 8080
          protocol: TCP
```

Apply and verify:

```bash
oc apply -f bridge-netpol-dmz.yaml -n kafka-http-bridge
oc get networkpolicies -n kafka-http-bridge
```

Verify the lockdown behaves as intended:

```bash
# From the api-gateway namespace: connection succeeds (replace with your API layer's namespace)
oc run bridge-allowed --rm -i --restart=Never --image=curlimages/curl -n api-gateway -- \
  --max-time 5 -s -o /dev/null -w "%{http_code}\n" \
  http://my-bridge-bridge-service.kafka-http-bridge.svc.cluster.local:8080/health

# From an unrelated namespace (e.g., default): connection should TIME OUT
oc run bridge-blocked --rm -i --restart=Never --image=curlimages/curl -n default -- \
  --max-time 5 -s -o /dev/null -w "%{http_code}\n" \
  http://my-bridge-bridge-service.kafka-http-bridge.svc.cluster.local:8080/health
```

Notes:

- **Egress is untouched:** these policies restrict ingress only, so the bridge can still connect to the proxy's bootstrap (`kafka-proxy:9082`) and, if configured, the Keycloak issuer. If you later add a default-deny **egress** policy to `kafka-http-bridge`, remember to allow egress to `kafka-proxy` on the proxy's bootstrap port.
- **Cluster Operator:** the operator reconciles the bridge via the Kubernetes API and pod-local probes — it does not need network access to port 8080, so the default-deny does not affect reconciliation.
- **Expose the bridge through your API layer:** front the `my-bridge-bridge-service` with your gateway (in the allowed namespace) rather than exposing the bridge directly — the gateway can add the OAuth 2.0 authentication the HTTP interface lacks, obtaining tokens for HTTP clients with the `kafka-api-gateway` client from Step 1.4 (see the note in 12.4).

## Troubleshooting Tips

- **Operator not ready** — check the deployment and pod logs in the operator namespace:

  ```bash
  oc get deployments,pods -n kafka-proxy
  oc logs -n kafka-proxy deploy/kroxylicious-operator
  ```
- **Virtual cluster not becoming ready** — `KafkaProxyIngress`/`VirtualKafkaCluster` resources must reference resources **in the same namespace**; verify `proxyRef`, `targetKafkaServiceRef`, and `ingressRef` names.
- **Client connection refused / TLS handshake failures** — almost always certificate SAN coverage: every bootstrap *and* per-broker address clients use must be in the certificate.
- **Clients connect but can't reach brokers** — `nodeIdRanges` must cover every broker ID in the target cluster; also check DNS for `loadBalancer` broker addresses (each broker needs a unique, resolvable name).
- **`openShiftRoute` rejected** — the operator checks for the OpenShift Route API; this ingress type is only valid on OpenShift, not plain Kubernetes.
- **Missing KMS key with Record Encryption** — records silently pass through unencrypted; watch the proxy metrics to catch this.
- **HTTP Bridge not reconciled** — the Cluster Operator must watch the bridge's namespace; check `STRIMZI_NAMESPACE` (Step 12.1) and the RoleBindings in `kafka-http-bridge`.
- **HTTP Bridge connects but produce fails** — verify `bootstrapServers` points at the proxy's clusterIP bootstrap (not the Kafka cluster's), and that the bridge trusts the proxy's CA when the ingress uses TLS.
