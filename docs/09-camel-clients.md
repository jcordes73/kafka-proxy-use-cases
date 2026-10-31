# Step 9: Deploy Camel Producer and Consumer Clients (kafka-producer and kafka-consumer namespaces)

> [↑ Setup overview](../openshift-setup.md) · prev: [Step 8: Console](08-console.md) · next: [Step 10: Custom filter](10-custom-filter.md)

To complete the picture, deploy two **Camel Quarkus** integrations — a Kafka **producer** route (timer-driven) in a `kafka-producer` namespace and a Kafka **consumer** route in a `kafka-consumer` namespace — each in its own namespace for logical separation, connecting to the **proxy's virtual cluster** like any other client and never directly to the brokers.

> Red Hat build of Apache **Camel K is no longer supported** (last release 1.10.10, Camel 3.18). The latest supported release is **Red Hat build of Apache Camel for Quarkus 3.33.3** (Camel **4.18.3.redhat-00001**, Camel Quarkus 3.33.0.redhat-00010) — see the [Camel release schedule](https://access.redhat.com/articles/7021827) for the current version, and the [Camel for Quarkus reference](https://docs.redhat.com/en/documentation/red_hat_build_of_apache_camel/) for details.

## 9.1 — Create the namespaces and the Camel Quarkus projects

Create one namespace per client:

```bash
oc new-project kafka-producer
oc new-project kafka-consumer
```

Create two integration projects (one per client), each pinned to the latest Red Hat Camel for Quarkus release:

```xml
<!-- pom.xml (essentials) -->
<properties>
  <maven.compiler.release>21</maven.compiler.release>
  <quarkus.platform.version>3.33.3.SP2-redhat-00001</quarkus.platform.version>
</properties>

<repositories>
  <repository>
    <id>redhat-ga</id>
    <url>https://maven.repository.redhat.com/ga/</url>
  </repository>
</repositories>

<dependencyManagement>
  <dependencies>
    <dependency>
      <groupId>com.redhat.quarkus.platform</groupId>
      <artifactId>quarkus-camel-bom</artifactId>
      <version>${quarkus.platform.version}</version>
      <type>pom</type>
      <scope>import</scope>
    </dependency>
  </dependencies>
</dependencyManagement>

<dependencies>
  <dependency>
    <groupId>org.apache.camel.quarkus</groupId>
    <artifactId>camel-quarkus-kafka</artifactId>
  </dependency>
  <dependency>
    <groupId>org.apache.camel.quarkus</groupId>
    <artifactId>camel-quarkus-timer</artifactId>
  </dependency>
</dependencies>
```

> **⚠️ `quarkus-maven-plugin` version:** the plugin is versioned by **Quarkus core**, not by the Red Hat platform BOM — using `${quarkus.platform.version}` (`3.33.3.SP2-redhat-00001`) fails plugin resolution. Take the core version pinned inside the BOM (`io.quarkus:quarkus-core`, here `3.33.3.redhat-00007`):
>
> ```xml
> <properties>
>   <quarkus.plugin.version>3.33.3.redhat-00007</quarkus.plugin.version>
> </properties>
> <!-- ... -->
> <plugin>
>   <groupId>io.quarkus</groupId>
>   <artifactId>quarkus-maven-plugin</artifactId>
>   <version>${quarkus.plugin.version}</version>
> </plugin>
> ```

## 9.2 — Producer and consumer routes (separate projects)

**Producer project** (deployed to `kafka-producer`):

```java
// producer: src/main/java/org/example/KafkaProducerRoute.java
package org.example;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import jakarta.enterprise.context.ApplicationScoped;
import org.apache.camel.builder.RouteBuilder;

@ApplicationScoped
public class KafkaProducerRoute extends RouteBuilder {
    @Override
    public void configure() {
        // (OAuth client-config keys set programmatically — see Step 9.3)

        from("timer:tick?period=5s")
            // Message body conforms to the record-validation demo JSON schema
            // (Step 5.4 / Demo 2): {"orderId": string, "amount": number},
            // no additional properties.
            .process(e -> {
                int amount = ThreadLocalRandom.current().nextInt(1, 1000);
                String orderId = "C-" + UUID.randomUUID().toString().substring(0, 8);
                e.getIn().setBody("{\"orderId\":\"" + orderId + "\",\"amount\":" + amount + "}");
            })
            .to("kafka:topic-A")   // produced THROUGH the proxy
            .log("sent: ${body}");
    }
}
```

**Consumer project** (deployed to `kafka-consumer`):

```java
// consumer: src/main/java/org/example/KafkaConsumerRoute.java
package org.example;

import jakarta.enterprise.context.ApplicationScoped;
import org.apache.camel.builder.RouteBuilder;

@ApplicationScoped
public class KafkaConsumerRoute extends RouteBuilder {
    @Override
    public void configure() {
        from("kafka:topic-A?groupId=camel-consumer&autoOffsetReset=earliest")
            .log("received: ${body}");   // consumed THROUGH the proxy
    }
}
```

## 9.3 — Connect both clients to the proxy

The Camel Kafka component is configured through its own component property — **not** the plain Quarkus `kafka.bootstrap.servers` key (that one configures `quarkus-kafka`, which Camel Quarkus ignores). In **both** projects, point it at the **proxy's clusterIP bootstrap** (from [Step 6.10](06-proxy.md#610--apply-and-verify) — not the Kafka cluster's bootstrap). The environment override is `CAMEL_COMPONENT_KAFKA_BROKERS` (Quarkus maps property dots to underscores) — used by the Deployments in Step 9.4:

```properties
# src/main/resources/application.properties (in both projects)
camel.component.kafka.brokers=kafka-cluster-1-cluster-ip-1-bootstrap.kafka-proxy.svc.cluster.local:9292
```

The proxied listener requires **SASL/OAUTHBEARER** (OAUTHBEARER is the only mechanism on the `tls` listener — see [deploy/kafka/02-kafka.yaml](../deploy/kafka/02-kafka.yaml)), so set the OAuth options in **both** projects (the SASL exchange passes through the proxy to the brokers, which validate the token against Keycloak's JWKS):

```properties
camel.component.kafka.saslAuthType=OAUTH
camel.component.kafka.oauthClientId=camel-producer
camel.component.kafka.oauthClientSecret=${OAUTH_CLIENT_SECRET}
camel.component.kafka.oauthTokenEndpointUri=http://keycloak-http.kafka-auth.svc.cluster.local:8080/realms/kafka/protocol/openid-connect/token
camel.component.kafka.sslTruststoreLocation=/etc/proxy-ca/ca.crt
camel.component.kafka.sslTruststoreType=PEM
```

> `saslAuthType=OAUTH` derives `securityProtocol=SASL_SSL` and `saslMechanism=OAUTHBEARER` — no extra dependency needed (Apache Kafka's native OAuth handler is in `kafka-clients`). Credentials come from the `camel-producer`/`camel-consumer` Keycloak service-account clients ([deploy/keycloak/04-kafka-realm-import.yaml](../deploy/keycloak/04-kafka-realm-import.yaml)); the secret is injected via the `OAUTH_CLIENT_SECRET` env var — set it in the Deployments from a Secret created in the client's namespace (Step 9.4). Kafka 4.x rejects **http** token endpoints unless allowlisted, so the Deployments also set `-Dorg.apache.kafka.sasl.oauthbearer.allowed.urls=<token-endpoint-url>` via `JAVA_OPTS_APPEND`; pointing the endpoint at the Keycloak **https** route removes the need (but then the JVM must trust Keycloak's CA). For production, inject credentials via a Kubernetes Secret and a configuration provider rather than a plaintext property.

## 9.4 — Build and deploy the containers

Build one image per client:

```bash
# In the producer project:
mvn -B package -DskipTests
podman build -t <registry>/camel-kafka-producer:1.0.0 -f - . <<'EOF'
FROM registry.access.redhat.com/ubi9/openjdk-21-runtime:latest
COPY target/quarkus-app /deployments/
EOF
podman push <registry>/camel-kafka-producer:1.0.0

# In the consumer project:
mvn -B package -DskipTests
podman build -t <registry>/camel-kafka-consumer:1.0.0 -f - . <<'EOF'
FROM registry.access.redhat.com/ubi9/openjdk-21-runtime:latest
COPY target/quarkus-app /deployments/
EOF
podman push <registry>/camel-kafka-consumer:1.0.0
```

> **No external registry?** Build in-cluster with an OpenShift `BuildConfig` (binary build + Docker strategy) instead of `podman push` — the internal registry serves the images via `image-registry.openshift-image-registry.svc:5000`. Ready-to-apply BuildConfig + ImageStream manifests are in [`deploy/camel/`](../deploy/camel):
>
> ```bash
> # Per client (producer shown; repeat in kafka-consumer with consumer names):
> mvn -B package -DskipTests
> oc apply -f deploy/camel/producer/00-buildconfig.yaml -n kafka-producer
> oc start-build camel-kafka-producer --from-dir=deploy/camel/producer -n kafka-producer --follow
> ```
>
> The `--from-dir` payload carries the `Dockerfile` **and** `target/quarkus-app/`; the image lands at `image-registry.openshift-image-registry.svc:5000/kafka-producer/camel-kafka-producer:1.0.0` (`lookupPolicy.local: true` lets the Deployment reference it without the registry prefix).

Deploy each client with a `Deployment` — the ready-to-apply manifests are [`deploy/camel/producer/01-deployment.yaml`](../deploy/camel/producer/01-deployment.yaml) and [`deploy/camel/consumer/01-deployment.yaml`](../deploy/camel/consumer/01-deployment.yaml) (for the in-cluster BuildConfig image path above). For the external-registry path, the only difference is `image: <registry>/camel-kafka-{producer,consumer}:1.0.0`.

Field notes (both Deployments are identical apart from the image and the OAuth client):

- `CAMEL_COMPONENT_KAFKA_BROKERS` — overrides `camel.component.kafka.brokers` (Step 9.3).
- `OAUTH_CLIENT_ID` / `OAUTH_CLIENT_SECRET` — the `camel-producer`/`camel-consumer` Keycloak service-account client and its Secret (`camel-producer-oauth`/`camel-consumer-oauth`, key `client-secret`, created in the client's namespace — the realm import does not create Secrets).
- `JAVA_OPTS_APPEND` — Kafka 4.x rejects **http** token endpoints unless allowlisted: `-Dorg.apache.kafka.sasl.oauthbearer.allowed.urls=<token-endpoint-url>`.
- TLS + SASL options (`saslAuthType`, `oauthTokenEndpointUri`, `sslTruststore*`) are camelCase component options — they cannot be expressed as `CAMEL_COMPONENT_KAFKA_*` env vars (underscore mapping would produce invalid Kafka option names), so they live in the image's `application.properties` (Step 9.3); only the CA mount and the OAuth secret live here.

Apply and watch the routes run (each in its own namespace):

```bash
oc apply -f deploy/camel/producer/01-deployment.yaml
oc apply -f deploy/camel/consumer/01-deployment.yaml

oc wait --for=condition=Available deployment/camel-kafka-producer -n kafka-producer --timeout=300s
oc wait --for=condition=Available deployment/camel-kafka-consumer -n kafka-consumer --timeout=300s

oc logs -f deployment/camel-kafka-producer -n kafka-producer
oc logs -f deployment/camel-kafka-consumer -n kafka-consumer
```

Expected output (consumer logs — JSON records conforming to the record-validation demo schema):

```text
received: {"orderId":"C-3f8a91d2","amount":742}
received: {"orderId":"C-b204e7c5","amount":318}
```

## 9.5 — Operational notes

- **Namespace separation:** each client lives in its own namespace (`kafka-producer`, `kafka-consumer`) — own RBAC, own service accounts, and independently lockable via NetworkPolicies.
- **DMZ coherence:** the Camel clients connect only to the proxy's virtual cluster — direct access from `kafka-producer` or `kafka-consumer` to the Kafka pods is denied by the [Step 3.4](03-kafka-cluster.md#34--restrict-kafka-ingress-to-the-proxy-namespace-dmz) NetworkPolicies. The producer's messages and the consumer's records both flow through the proxy, so any enabled filters (encryption, validation, authorization) apply to this traffic too.
- **Schema-conformant payloads:** the producer generates records matching the record-validation demo JSON schema (Step 5.4 — `orderId` string, `amount` number, no additional properties). The same payload passes the RecordValidation filter scoped to `topic-A` ([Demo 2](11-demos.md#demo-2--record-validation-end-to-end)), and the JSON records make `topic-A` consumable in JSON format through the HTTP Bridge (the bridge's JSON decoder rejects non-JSON records with 406).
- **Verify in the console ([Step 8](08-console.md#step-8-deploy-the-streams-for-apache-kafka-console-kafka-console-namespace)):** the produced messages appear on `topic-A` in the Streams for Apache Kafka Console — a nice end-to-end check that everything shares one data path.
- **Scaling:** the consumer uses a fixed consumer group (`camel-consumer`); scale the Deployment for multiple partitions and rebalance like any Kafka consumer.
- **Version pinning:** the BOM above pins the Red Hat build; check the [release schedule](https://access.redhat.com/articles/7021827) before upgrading — Camel Quarkus and Quarkus platform versions move together.
