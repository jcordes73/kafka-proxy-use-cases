# Step 10: Develop and Deploy a Custom Filter (Optional)

> [↑ Setup overview](../openshift-setup.md) · prev: [Step 9: Camel clients](09-camel-clients.md) · next: [Step 11: Demo scenarios](11-demos.md)

Beyond the built-in filters ([use case catalog](../README.md#use-case-catalog)), the proxy's behavior can be extended with your own Java filter — for example custom redaction, routing, or enrichment logic that no shipped filter provides.

**Where this is documented.** The Red Hat 3.2 documentation covers the *deployment* side of filters — the `KafkaProtocolFilter` resource and the `filterRefs` wiring ([Step 6.6](06-proxy.md#66--optional-add-a-filter); [Deploying and Managing → Deploying a proxy](https://docs.redhat.com/en/documentation/red_hat_streams_for_apache_kafka/3.2/html/deploying_and_managing_streams_for_apache_kafka_proxy_on_openshift)) — but **does not publish a filter-development guide**: the [product Overview](https://docs.redhat.com/en/documentation/red_hat_streams_for_apache_kafka/3.2/html/streams_for_apache_kafka_on_openshift_overview) only notes that "an API provides a convenient means for implementing custom logic". Filter development is documented by the upstream Kroxylicious project, whose 0.25.1 release matches the operator and proxy versions deployed by this guide: the [Developer Quick Start](https://kroxylicious.io/documentation/0.25.1/html/developer-quick-start), the [Developer Guide](https://kroxylicious.io/documentation/0.25.1/html/developer-guide), and the [Javadocs](https://kroxylicious.io/documentation/0.25.1/javadoc/index.html).

The end-to-end path: generate a filter project → implement and test → package the plugin JARs → build a custom proxy image → point the operator at it → wire the filter to the virtual cluster.

## 10.1 — Generate and test a filter project

Requires **JDK 21** and Maven. The `kroxylicious-filter-archetype` Maven archetype bootstraps a project with two working find-and-replace filters to experiment with:

```bash
mvn archetype:generate -DarchetypeGroupId=io.kroxylicious \
  -DarchetypeArtifactId=kroxylicious-filter-archetype \
  -DarchetypeVersion=0.25.1 \
  -DgroupId=org.example -DartifactId=sample-filter \
  -Dversion=1.0-SNAPSHOT -DinteractiveMode=false
cd sample-filter
mvn verify
```

`mvn verify` runs the generated unit and integration tests — the integration tests run against an **in-VM Kafka cluster**, so no broker infrastructure is needed while iterating on filter logic. The two sample filters:

- `SampleProduceRequestFilterFactory` — rewrites produce requests in flight (default `foo` → `bar`)
- `SampleFetchResponseFilterFactory` — rewrites fetch responses (default `bar` → `baz`)

## 10.2 — Implement your own filter

- Protocol filters implement interfaces from `io.kroxylicious:kroxylicious-api` (on Maven Central); knowledge of the Kafka protocol is generally required. The [Javadocs](https://kroxylicious.io/documentation/0.25.1/javadoc/index.html) are the API reference.
- Each filter ships with a **`FilterFactory`** that constructs the filter from configuration — the values later carried in `spec.configTemplate` — and is registered through the standard Java **ServiceLoader** mechanism (`META-INF/services/io.kroxylicious.proxy.filter.FilterFactory`).
- The `spec.type` of a `KafkaProtocolFilter` ([Step 6.6](06-proxy.md#66--optional-add-a-filter)) must name the factory the ServiceLoader exposes: the sample project's `SampleProduceRequestFilterFactory` / `SampleFetchResponseFilterFactory`, or built-in short names such as `RecordValidation` and `SaslInspection`.

## 10.3 — Package the plugin JARs

Filters are packaged as standard JARs: the filter classes, one `FilterFactory` per filter, and the ServiceLoader metadata. Third-party plugin JARs are deployed through the **`classpath-plugins/`** directory under the proxy installation directory — the startup script scans its subdirectories and adds their contents to the classpath, where plugins are discovered via ServiceLoader alongside the built-in filters. Each plugin lives in its own subdirectory, together with any third-party dependency JARs it needs:

```text
kroxylicious/
├── libs/                      # built-in proxy JARs (do not modify)
└── classpath-plugins/         # third-party plugin JARs, one subdir per plugin
    └── sample-filter/
        └── sample-filter-1.0.jar
```

> ⚠️ **Loading plugins from `classpath-plugins/` is an Alpha feature** — it may change or be removed without warning in any release. There is also **no classloader isolation between plugins**: all plugins share a single classpath with the proxy itself, so shade (relocate) transitive dependencies with the Maven Shade Plugin to avoid version collisions with the proxy or other plugins.

The 0.25.1 proxy image already ships the (empty) `/opt/kroxylicious/classpath-plugins/` directory — a custom image only needs to add files to it. (If the directory is missing, plugin scanning is skipped and the proxy starts normally.)

## 10.4 — Build the custom proxy image in-cluster

Layer the plugin JARs onto the stock proxy image, using the same in-cluster Build pattern as the Camel clients ([Step 9.4](09-camel-clients.md#94--build-and-deploy-the-containers)). [`deploy/custom-filter/Dockerfile`](../deploy/custom-filter/Dockerfile):

```dockerfile
FROM quay.io/kroxylicious/proxy:0.25.1

COPY target/*.jar /opt/kroxylicious/classpath-plugins/sample-filter/
```

[`deploy/custom-filter/01-buildconfig.yaml`](../deploy/custom-filter/01-buildconfig.yaml) declares the `kafka-filter-dev` namespace, an `ImageStream`, and a binary (from-dir) Docker-strategy `BuildConfig` producing `kroxylicious-custom-filter:0.25.1`:

```bash
cp deploy/custom-filter/Dockerfile sample-filter/
oc apply -f deploy/custom-filter/01-buildconfig.yaml
oc start-build kroxylicious-custom-filter --from-dir=sample-filter \
  -n kafka-filter-dev --follow
```

The payload is the Maven project directory — the build needs the `Dockerfile` plus `target/*.jar`. The image is then referenceable cluster-internally as:

```text
image-registry.openshift-image-registry.svc:5000/kafka-filter-dev/kroxylicious-custom-filter:0.25.1
```

## 10.5 — Point the operator at the custom image

The `KafkaProxy` CR has no image-override field; the operator picks the operand image from its own configuration. The 0.25.1 operator honors the **`KROXYLICIOUS_IMAGE`** environment variable on the operator Deployment to override it (verified in the operator source for this release; neither the Red Hat 3.2 guide nor the upstream operator guide documents it):

```bash
oc -n kafka-proxy set env deploy/kafka-proxy \
  KROXYLICIOUS_IMAGE=image-registry.openshift-image-registry.svc:5000/kafka-filter-dev/kroxylicious-custom-filter:0.25.1
oc -n kafka-proxy rollout status deploy/kafka-proxy --timeout=120s
```

The operator then recreates the proxy deployment (`simple`) with the custom image. Verify the plugin JAR is in place:

```bash
oc -n kafka-proxy rollout status deploy/simple --timeout=120s
oc -n kafka-proxy exec deploy/simple -- ls /opt/kroxylicious/classpath-plugins/sample-filter/
```

> The operator does not validate the operand image — keep the custom image's base in lockstep with the operator version: rebuild from the matching `quay.io/kroxylicious/proxy` tag whenever you upgrade the operator ([Step 6.1](06-proxy.md#61--install-the-proxy-operator)). `KROXYLICIOUS_IMAGE` is deliberately not baked into [`deploy/proxy/01-proxy-operator-install.yaml`](../deploy/proxy/01-proxy-operator-install.yaml) — the base stack runs the stock image.

## 10.6 — Configure and wire the filter

Declare the filter as a `KafkaProtocolFilter` ([Step 6.6](06-proxy.md#66--optional-add-a-filter)) and reference it from the virtual cluster's `filterRefs`. [`deploy/custom-filter/02-kafkaprotocolfilter.yaml`](../deploy/custom-filter/02-kafkaprotocolfilter.yaml):

```yaml
kind: KafkaProtocolFilter
apiVersion: kroxylicious.io/v1alpha1
metadata:
  namespace: kafka-proxy
  name: sample-produce-request-filter
spec:
  type: SampleProduceRequestFilterFactory
  configTemplate:
    findValue: foo
    replacementValue: bar
```

```bash
oc apply -f deploy/custom-filter/02-kafkaprotocolfilter.yaml
oc -n kafka-proxy patch virtualkafkacluster kafka-cluster-1 --type=merge \
  -p '{"spec":{"filterRefs":[{"name":"sample-produce-request-filter"}]}}'
oc -n kafka-proxy rollout status deploy/simple --timeout=120s   # proxy reconfigures
```

Verify end to end: produce a message containing `foo` through the proxy (the [Step 9](09-camel-clients.md#step-9-deploy-camel-producer-and-consumer-clients-kafka-producer-and-kafka-consumer-namespaces) producer, or a console client against the [Step 6.10](06-proxy.md#610--apply-and-verify) bootstrap) and consume it — the stored value reads `bar`. If the proxy pod crash-loops after wiring, the `spec.type` name does not match what the ServiceLoader exposes — check `oc -n kafka-proxy logs deploy/simple` for filter resolution errors.

> Reverting: remove the `filterRefs` entry and delete the `KafkaProtocolFilter`, then `oc -n kafka-proxy set env deploy/kafka-proxy KROXYLICIOUS_IMAGE-` to drop the override and return to the stock image.
