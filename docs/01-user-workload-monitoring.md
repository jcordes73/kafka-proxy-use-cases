# Step 1: Integrate the Stack with OpenShift User-Workload Monitoring (Observability)

> [↑ Setup overview](../openshift-setup.md) · next: [Step 2: Keycloak (kafka-auth)](02-keycloak.md)

The stack above has one component per capability — brokers, proxy, bridge, identity provider, UI, clients — but no common place to look at how they all behave together. OpenShift ships a **built-in monitoring stack** that is the natural home for that: enable **monitoring for user-defined projects** (user-workload monitoring), and every component's Prometheus metrics are collected centrally by the user-workload Prometheus instance (`prometheus-user-workload`), visualized on the Observe → Dashboard page, and available for Alertmanager rules.

> **⚠️ Deployment order — deploy 1.1 FIRST.** The user-workload monitoring stack (Step 1.1) is the **first deployment of the guide** — before Keycloak (Step 2) and the Kafka cluster (Step 3). It only needs `cluster-admin` and has no component dependencies, and deploying it first means every component's metrics are collected from the moment it deploys. The per-component integration below (1.2–1.9) is deployed **last**, after those components exist — the [setup overview](../openshift-setup.md) Guide map gives the full deployment order.

This step wires **every component of the stack** into that stack:

| # | Component | Metrics source | Integration mechanism | Scrape target |
|---|---|---|---|---|
| 15.3 | Kafka cluster (brokers/KRaft nodes) | Strimzi Metrics Reporter | `Kafka` CR `metricsConfig` + `PodMonitor` | port `tcp-prometheus` (9404), `/metrics` |
| 15.4 | Kafka Proxy + proxy Operator | built-in `/metrics` endpoints | `PodMonitor` | proxy: port `management`; operator: port `http` |
| 15.5 | HTTP Bridge | Prometheus JMX Exporter | `KafkaBridge` CR `metricsConfig` + `PodMonitor` | port `rest-api` (8080), `/metrics` |
| 15.6 | Red Hat build of Keycloak | Micrometer | Keycloak CR `metrics-enabled` option | auto-generated `ServiceMonitor` |
| 15.7 | Streams for Apache Kafka Console | consumes cluster metrics | Console CR `metricsSources` | reads from the user-workload Prometheus |
| 15.8 | Camel producer/consumer | Quarkus Micrometer | Micrometer deps + `Service`/`ServiceMonitor` | port 8080, `/q/metrics` |

Prerequisites and caveats:

- **1.1 needs `cluster-admin`** (it configures the cluster monitoring stack) and only ever happens once. It is deployed **first** — before Keycloak and the Kafka cluster (see the deployment-order callout above). Everything after that is namespace-level work and is deployed once the components exist.
- The `ServiceMonitor`/`PodMonitor` CRDs must exist — they are installed by default on OpenShift and become active once user-workload monitoring is enabled.
- **The lockdown NetworkPolicies must allow scraping.** A `PodMonitor` scrape goes from the monitoring pods directly to each pod's metrics port, so the default-deny policies of the `kafka` and `kafka-http-bridge` namespaces ([Step 3.4](03-kafka-cluster.md#34--restrict-kafka-ingress-to-the-proxy-namespace-dmz) and 12.5) would block Prometheus entirely. Rule `15.2` opens them for the monitoring namespace only.
- Metrics endpoints are **HTTP and unauthenticated** — that is exactly why the DMZ restrictions above matter.

## 1.1 — Enable monitoring for user-defined projects (deploy this first; cluster admin, once)

Monitoring of user-defined projects is enabled through the `cluster-monitoring-config` ConfigMap in `openshift-monitoring`. This is the **first deployment of the guide** — it comes before Keycloak (Step 2) and the Kafka cluster (Step 3); see the deployment-order callout at the top of this document. The ready-to-apply manifest is [`deploy/observability/00-cluster-monitoring-config.yaml`](../deploy/observability/00-cluster-monitoring-config.yaml):

```yaml
# deploy/observability/00-cluster-monitoring-config.yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: cluster-monitoring-config
  namespace: openshift-monitoring
data:
  config.yaml: |
    enableUserWorkload: true
```

```bash
oc apply -f deploy/observability/00-cluster-monitoring-config.yaml
```

Wait for the user-workload monitoring components to appear (this can take a couple of minutes):

```bash
oc -n openshift-user-workload-monitoring get pod
```

Expect `prometheus-operator`, `prometheus-user-workload-0`, and `thanos-ruler-user-workload-*` pods to be `Running`.

> Configuring the user-workload stack itself (retention, resource limits, Alertmanager) is done via the `user-workload-monitoring-config` ConfigMap in `openshift-user-workload-monitoring` — see the [Configuring user workload monitoring](https://docs.redhat.com/en/documentation/monitoring_stack_for_red_hat_openshift/4.22/html/configuring_user_workload_monitoring) guide.

## 1.2 — Allow the monitoring stack into the locked-down namespaces

Two of the locked-down namespaces ([Step 3.4](03-kafka-cluster.md#34--restrict-kafka-ingress-to-the-proxy-namespace-dmz) and [Step 7.5](07-http-bridge.md#75--restrict-http-access-to-the-bridge-dmz-networkpolicy)) would block metric scraping. Add one additive policy per namespace, allowing ingress **only** from the monitoring namespace and **only** on the metrics ports. Kubernetes `NetworkPolicy` rules are additive, so these extend the existing DMZ rules without loosening anything else.

The ready-to-apply manifest is [`deploy/observability/01-metrics-netpols.yaml`](../deploy/observability/01-metrics-netpols.yaml) — an allowance for each locked-down namespace (the Kafka cluster namespaces in the secure backend on the 9404 metrics port, `kafka-http-bridge` in the DMZ on 8080):

```bash
oc apply -f deploy/observability/01-metrics-netpols.yaml
oc get networkpolicies -n kafka-cluster-1
oc get networkpolicies -n kafka-http-bridge
```

Notes:

- The `kafka-proxy`, `kafka-console`, `kafka-producer`, `kafka-consumer`, and `kafka-auth` namespaces from this guide have no ingress policies, so no extra rules are needed there. If you later apply default-deny ingress to any of them (defense in depth), add the same allowance for `openshift-user-workload-monitoring` on that component's metrics port.
- The scrape arrives as **ingress** traffic to the monitored pods, so these policies are all you need — the monitoring pods' own egress configuration is managed by the cluster monitoring operator. If you later default-deny **egress** in a user namespace, that does not affect outbound scrape behaviour from `openshift-user-workload-monitoring` either.

## 1.3 — Kafka cluster: expose broker metrics and scrape them

For a Streams for Apache Kafka cluster, enable the **Strimzi Metrics Reporter** — it exposes Kafka broker/KRaft metrics directly in Prometheus format on port 9404 with no extra agent. The `metricsConfig` is already on the `Kafka` resource in [`deploy/kafka/02-kafka.yaml`](../deploy/kafka/02-kafka.yaml) (`spec.kafka.metricsConfig: type: strimziMetricsReporter`, `allowList: ".*"` — narrow it to e.g. `kafka.log.*`, `kafka.network.*`, `kafka.server.*` if needed):

```bash
oc apply -f deploy/kafka/02-kafka.yaml -n kafka-cluster-1

# Metrics config changes are rolled onto the broker pods
oc wait --for=condition=Ready kafka/kafka-cluster-1 -n kafka-cluster-1 --timeout=600s
```

Define the scrape: a `PodMonitor` selecting the cluster's pods (`strimzi.io/kind: Kafka`), mapping the Strimzi labels onto Prometheus labels (this mirrors the `kafka-resources-metrics.yaml` example shipped with Streams for Apache Kafka). The ready-to-apply manifest is [`deploy/observability/02-kafka-podmonitor.yaml`](../deploy/observability/02-kafka-podmonitor.yaml):

```bash
oc apply -f deploy/observability/02-kafka-podmonitor.yaml
```

Notes:

- Alternatively, apply the example `kafka-metrics.yaml` from the `streams-3.2-ocp-install-examples.zip` archive — it uses the Prometheus **JMX Exporter** flavor of `metricsConfig` with curated relabeling rules for the Strimzi Grafana dashboards (port 9404 in both cases).
- The example archive also ships PodMonitor definitions (`examples/metrics/prometheus-install/pod-monitors/`) for the **Cluster Operator** — label `strimzi.io/kind: cluster-operator`, port `http` — and **Entity Operator** (port `healthcheck`) if you want operator-level metrics too; both run in the `kafka` namespace in this setup.
- Strimzi provides **Grafana dashboards** calibrated to these metrics (`examples/metrics/grafana-dashboards/`) — import them into your Grafana instance, or visualize directly in Observe → Dashboards.

Verify with a PromQL query (Observe → Metrics, or against `prometheus-user-workload`):

```text
kafka_server_replicamanager_underreplicatedpartitions
```

## 1.4 — Kafka Proxy and proxy Operator: scrape via PodMonitor

The proxy emits Prometheus metrics for connections, messages, and per-filter activity (Record Encryption operations, validation rejections, idle-disconnect causes); the operator exposes its own reconciliation metrics. Both expose unauthenticated `<port>/metrics` endpoints — integrate both with user-workload monitoring as documented in the proxy guide. The ready-to-apply manifest is [`deploy/observability/03-proxy-podmonitors.yaml`](../deploy/observability/03-proxy-podmonitors.yaml) — two `PodMonitor` resources in `kafka-proxy`: `proxy` (selector `app.kubernetes.io/component: proxy`, port `management`) and `operator` (selector `...component: operator`, port `http`):

```bash
oc apply -f deploy/observability/03-proxy-podmonitors.yaml
```

Verify with:

```text
kroxylicious_build_info
```

Useful proxy signals to watch (labelled `virtual_cluster` and `node_id`):

- `kroxylicious_client_to_proxy_active_connections` vs `_disconnects_total`/`_errors_total` — client churn, including `idle_timeout` disconnections from the idle connection handling (use case [8 in the guide](../README.md#8-observability--metrics)).
- `kroxylicious_client_to_proxy_request_total` vs `kroxylicious_proxy_to_server_request_total` — a **short-circuiting filter** (Record Validation rejecting records) shows as downstream counts exceeding upstream counts; request-size deltas reveal the Record Encryption overhead.

> Full metric reference: [Monitoring](https://docs.redhat.com/en/documentation/red_hat_streams_for_apache_kafka/3.2/html/deploying_and_managing_streams_for_apache_kafka_proxy_on_openshift/assembly-operator-monitoring-operator) in the proxy guide.

## 1.5 — HTTP Bridge: enable and scrape bridge metrics

The bridge's Kafka producer/consumer internals are JMX-based, so use the Prometheus **JMX Exporter** flavor of `metricsConfig` (note: `.spec.enableMetrics` in `KafkaBridge` is deprecated and replaced by `metricsConfig`). The ready-to-apply manifest is [`deploy/observability/04-bridge-metrics.yaml`](../deploy/observability/04-bridge-metrics.yaml) — the `metricsConfig` (`type: jmxPrometheusExporter`, `valueFrom` the `bridge-metrics` ConfigMap) plus the ConfigMap with the rewrite rules (Kafka producer/consumer client metrics with topic context; more patterns in the shipped `kafka-bridge-metrics.yaml` example from the `streams-3.2-ocp-install-examples.zip` archive):

```bash
oc apply -f deploy/observability/04-bridge-metrics.yaml
```

The bridge metrics are exposed on the same HTTP port (8080, port name `rest-api`) as the REST API — the Step 1.2 policy already allows the monitoring stack to scrape it. The same manifest carries the `bridge-metrics` `PodMonitor` (selector `strimzi.io/kind: KafkaBridge`, port `rest-api`). Note that the manifest re-declares the `KafkaBridge` with the **full Step 7.2 spec** (TLS + OAUTHBEARER authentication, [deploy/bridge/01-kafka-bridge.yaml](../deploy/bridge/01-kafka-bridge.yaml)) plus `metricsConfig` — keep the two in sync, since applying a trimmed copy would strip the bridge's TLS/authentication settings.

Verify with the bridge's own metrics, e.g. `strimzi_bridge_kafka_producer_request_total`.

> The shipped example (`kafka-bridge-metrics.yaml`) contains the full rule set, including OAuth 2.0 client metrics — useful if you later enable token-based authentication on the data path ([Step 2.5](02-keycloak.md#25--client-authentication-at-the-proxy)).

## 1.6 — Keycloak: enable metrics (auto-generated ServiceMonitor)

The Red Hat build of Keycloak exposes Micrometer metrics on its management endpoint once enabled. The `metrics-enabled` option is already set on the Keycloak CR in [`deploy/keycloak/03-keycloak.yaml`](../deploy/keycloak/03-keycloak.yaml) (`spec.additionalOptions`); apply it and the operator auto-creates a `ServiceMonitor` for user-workload monitoring:

```bash
oc apply -f deploy/keycloak/03-keycloak.yaml
oc wait --for=condition=Ready keycloak/keycloak -n kafka-auth --timeout=600s
```

The Keycloak operator **automatically creates a `ServiceMonitor`** for the deployment when `metrics-enabled` is set (requires the `ServiceMonitor` CRD — present once Step 1.1 is done), targeting the Keycloak metrics endpoint on the management port. Verify and query:

```bash
oc get servicemonitors -n kafka-auth
```

Keys of interest: HTTP request timers on the metrics endpoint, and Keycloak runtime metrics (login/session counts, JWKS/introspection call stats) — the exact metric names follow Keycloak's Micrometer naming and depend on which build/profiling options are enabled.

> **NetworkPolicy note:** the Keycloak metrics endpoint is on the Keycloak **management** port (9000), distinct from the TLS ingress port. The `kafka-auth` namespace has no ingress policy in this guide. If you later default-deny ingress there, allow `openshift-user-workload-monitoring` on port 9000.

## 1.7 — Console (kafka-console): read metrics from user-workload monitoring

The Console displays cluster metrics, and without a metrics source it creates its own **embedded Prometheus** (fine for evaluation, bad for production) — swap it for the user-workload Prometheus from Steps 1.1–1.3. The ready-to-apply manifest [`deploy/console/02-kafka-console.yaml`](../deploy/console/02-kafka-console.yaml) already includes `metricsSources` (`uwm-prometheus`, `type: openshift-monitoring`) and `metricsSource: uwm-prometheus` on the cluster entry:

```bash
envsubst < deploy/console/02-kafka-console.yaml | oc apply -f -
oc -n kafka-console rollout status deploy/kafka-console-console-deployment --timeout=180s
```

The Console then shows the broker metrics collected by Steps 1.2–1.3 in its dashboard panels — from the same Prometheus that serves Observe → Dashboards, with no extra scraper running. (The `type` alternatives are `standalone` for a user-supplied Prometheus and `embedded` — the default for development.)

> **Scraping the console and its operator:** the `amq-streams-console` OLM operator ships **no** metrics Service or ServiceMonitor, and its console Service exposes only the UI port (3000) — where `/metrics` redirects to the sign-in flow. The console-api container serves Prometheus metrics at `/metrics` on port 8080, and the operator serves them at `/q/metrics` (also 8080), but neither is reachable through a Service out of the box. [`deploy/console/03-console-metrics.yaml`](../deploy/console/03-console-metrics.yaml) declares a dedicated metrics Service per component plus a scoped ServiceMonitor for each (the operator Service's selector embeds the CSV version — update it after approving an operator upgrade):
>
> ```bash
> oc apply -f deploy/console/03-console-metrics.yaml
> ```

## 1.8 — Camel producer/consumer: Micrometer + ServiceMonitor

The Camel Quarkus clients from [Step 9](09-camel-clients.md#step-9-deploy-camel-producer-and-consumer-clients-kafka-producer-and-kafka-consumer-namespaces) do not expose metrics by default — add the Quarkus **Micrometer** registry. Extend the dependency set of **both** projects (already in [`deploy/camel/producer/pom.xml`](../deploy/camel/producer/pom.xml) and the consumer pom):

```xml
<!-- pom.xml, after camel-quarkus-timer (both producer and consumer projects) -->
<dependency>
  <groupId>io.quarkus</groupId>
  <artifactId>quarkus-micrometer</artifactId>
</dependency>
<dependency>
  <groupId>io.quarkus</groupId>
  <artifactId>quarkus-micrometer-registry-prometheus</artifactId>
</dependency>
<dependency>
  <groupId>org.apache.camel.quarkus</groupId>
  <artifactId>camel-quarkus-micrometer</artifactId>
</dependency>
```

Rebuild and repush the images ([Step 9.4](09-camel-clients.md#94--build-and-deploy-the-containers)), then roll the Deployments. Register the metrics endpoints (`/q/metrics` on the app port) for scraping — the [Step 9](09-camel-clients.md#step-9-deploy-camel-producer-and-consumer-clients-kafka-producer-and-kafka-consumer-namespaces) Deployments have no Services yet, so create one per client plus a `ServiceMonitor`. The ready-to-apply manifests are [`deploy/camel/producer/02-metrics.yaml`](../deploy/camel/producer/02-metrics.yaml) and [`deploy/camel/consumer/02-metrics.yaml`](../deploy/camel/consumer/02-metrics.yaml) (a Service labelled for the ServiceMonitor selector plus a `ServiceMonitor` on `/q/metrics`):

```bash
oc apply -f deploy/camel/producer/02-metrics.yaml
oc apply -f deploy/camel/consumer/02-metrics.yaml
```

Verify with JVM + Quarkus + Kafka-client + Camel route metrics, e.g. `camel_route_timer_...`, `jvm_memory_used_bytes`, and Quarkus HTTP metrics for the Camel routes. This also gives you a client-side end-to-end delta against the proxy's `kroxylicious_*` counts from Step 1.4.

> The `/q/metrics` endpoint shares the app port with the routes (8080). The producer/consumer namespaces have no ingress policies in this guide; if you default-deny later, allow `openshift-user-workload-monitoring` on port 8080.

## 1.9 — Verify the whole path and add alert rules

**Verify scraping.** In the OpenShift web console: **Observe → Metrics** and pick one query per component — every one should return data rather than "no results":

```text
kafka_server_replicamanager_underreplicatedpartitions   # Step 1.3 — brokers
kroxylicious_build_info                                 # Step 1.4 — proxy
strimzi_bridge_kafka_producer_request_total             # Step 1.5 — bridge
up{namespace="kafka-auth"}                              # Step 1.6 — Keycloak reachable; its metric names vary by build (jvm_*, netty_*, ...)
up{namespace=~"kafka|kafka-proxy|kafka-http-bridge|kafka-producer|kafka-consumer|kafka-auth"}
```

**Alert rules.** Alert rules are defined as `PrometheusRule` resources in the component's namespace and are picked up by the user-workload Prometheus notification pipeline. Keep the rules component-specific and start minimal. The ready-to-apply manifest is [`deploy/observability/05-kafka-stack-alerts.yaml`](../deploy/observability/05-kafka-stack-alerts.yaml) — a `PrometheusRule` in `kafka-proxy` (with the `openshift.io/prometheus-rule-evaluation-scope: leaf-prometheus` label required by the user-workload Prometheus ruleSelector, OCP default) containing alerts for proxy availability (`up{namespace="kafka-proxy", pod=~"simple-.*"} == 0`) and broker health (`kafka_server_replicamanager_underreplicatedpartitions > 0`):

```bash
oc apply -f deploy/observability/05-kafka-stack-alerts.yaml
```

Rules worth adding as you enable filters (all derived from the counters gathered above):

- **Broker health:** `kafka_server_replicamanager_underreplicatedpartitions > 0`; Kafka Exporter, if deployed, adds consumer-lag alerts.
- **Proxy filter anomalies:** validation-rejection spikes (downstream request counts permanently exceeding upstream) and Record Encryption/DEK refresh errors — signal plans like "records passed through unencrypted".
- **Keycloak:** sustained cache-miss or request-failure rates on the metrics endpoint from Step 1.6.

> Configure notification receivers (email, webhook, etc.) in the `user-workload-monitoring-config` ConfigMap — the recipient routing is out of scope for this guide; see [Configuring metrics for user workload monitoring](https://docs.redhat.com/en/documentation/monitoring_stack_for_red_hat_openshift/4.22/html/configuring_user_workload_monitoring/configuring-metrics-uwm).
