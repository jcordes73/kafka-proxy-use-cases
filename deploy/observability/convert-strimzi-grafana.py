#!/usr/bin/env python3
"""Convert Strimzi Grafana dashboards (strimzi-kafka-operator) to Perses v1alpha2 CRs.

Uses the percli online-migrated output for panel chrome (TimeSeriesChart/StatChart
layout, thresholds, units) and injects the original Grafana PromQL queries with:
  - Kafka 4.x metric renames (verified against the live Thanos catalog)
  - label normalisation to the labels the cluster actually sets
    (namespace, strimzi_io_cluster, pod, ...)
  - datasource fixed to the `user-workload-monitoring` Perses datasource
  - Grafana's __rate_interval replaced with a fixed 5m window
Output: perses.dev/v1alpha2 PersesDashboard CRs (spec under spec.config), old-style
panels map keyed "row_col" (same verified shape as deploy/observability/08-*).
"""
import json
import re
import sys

import yaml

METRIC_RENAMES = {
    # Kafka 4.x renamed the *_total counters to *_persec_total (verified live)
    "kafka_server_brokertopicmetrics_bytesin_total": "kafka_server_brokertopicmetrics_bytesinpersec_total",
    "kafka_server_brokertopicmetrics_bytesout_total": "kafka_server_brokertopicmetrics_bytesoutpersec_total",
    "kafka_server_brokertopicmetrics_messagesin_total": "kafka_server_brokertopicmetrics_messagesinpersec_total",
    "kafka_server_brokertopicmetrics_failedfetchrequests_total": "kafka_server_brokertopicmetrics_failedfetchrequestspersec_total",
    "kafka_server_brokertopicmetrics_failedproducerequests_total": "kafka_server_brokertopicmetrics_failedproducerequestspersec_total",
    "kafka_server_brokertopicmetrics_totalfetchrequests_total": "kafka_server_brokertopicmetrics_totalfetchrequestspersec_total",
    "kafka_server_brokertopicmetrics_totalproducerequests_total": "kafka_server_brokertopicmetrics_totalproducerequestspersec_total",
    "kafka_controller_controllerstats_uncleanleaderelections_total": "kafka_controller_controllerstats_uncleanleaderelectionspersec_total",
    "kafka_server_replicamanager_isrshrinks_total": "kafka_server_replicamanager_isrshrinkspersec_total",
    "kafka_server_replicamanager_isrexpands_total": "kafka_server_replicamanager_isrexpandspersec_total",
    "kafka_server_replicamanager_failedisrupdates_total": "kafka_server_replicamanager_failedisrupdatespersec_total",
    "kafka_network_socketserver_networkprocessoravgidle_percent": "kafka_network_socketserver_networkprocessoravgidlepercent",
    "kafka_server_kafkarequesthandlerpool_requesthandleravgidle_percent": "kafka_server_kafkarequesthandlerpool_requesthandleravgidlepercent_total",
    # kraft: raftmetrics -> raft_metrics (underscored new naming)
    "kafka_server_raftmetrics_current_state": "kafka_server_raft_metrics_current_state_info",
    # raftchannelmetrics -> raft_channel_metrics, _total -> _rate
    "kafka_server_raftchannelmetrics_incoming_byte_total": "kafka_server_raft_channel_metrics_incoming_byte_rate",
    "kafka_server_raftchannelmetrics_outgoing_byte_total": "kafka_server_raft_channel_metrics_outgoing_byte_rate",
    "kafka_server_raftchannelmetrics_request_total": "kafka_server_raft_channel_metrics_request_rate",
    "kafka_server_raftchannelmetrics_response_total": "kafka_server_raft_channel_metrics_response_rate",
}

# labels the Grafana dashboards use vs the labels set on this cluster
LABEL_MAP = {
    "kubernetes_namespace": "namespace",
    "kubernetes_pod_name": "pod",
    "strimzi_connect_cluster_name": "strimzi_io_cluster",
    "strimzi_mirror_maker_cluster_name": "strimzi_io_cluster",
    "bridge_container_name": "strimzi_io_name",
    # kafka_cluster_name (label form, some dashboards) -> strimzi_io_cluster
    "strimzi_cluster_name": "strimzi_io_cluster",
}

# Grafana var name -> renamed var (queries are rewritten to the new name)
VAR_NAME = {
    "kubernetes_namespace": "namespace",
}

# Grafana var name -> label the variable should list on this cluster
VAR_LABEL = {
    "kubernetes_namespace": "namespace",
    "strimzi_cluster_name": "strimzi_io_cluster",
    "strimzi_connect_cluster_name": "strimzi_io_cluster",
    "strimzi_mirror_maker_cluster_name": "strimzi_io_cluster",
    "bridge_container_name": "strimzi_io_name",
    "kafka_broker": "pod",
    "kraft_node": "pod",
    "kafka_topic": "topic",
    "topic": "topic",
    "kafka_partition": "partition",
    "consumergroup": "consumergroup",
}

# metrics that the deployed components do NOT expose -> panels using only these are dropped
UNAVAILABLE_PREFIXES = (
    "strimzi_bridge_kafka_producer",
    "strimzi_bridge_kafka_consumer",
    "zookeeper",
    "zk_",
)

DATASOURCE = {"kind": "PrometheusDatasource", "name": "user-workload-monitoring"}

IGNORE_TOKENS = {
    "rate", "irate", "sum", "by", "avg", "max", "min", "increase", "count",
    "avg_over_time", "max_over_time", "min_over_time", "sum_over_time",
    "count_over_time", "topk", "bottomk", "histogram_quantile", "label_replace",
    "without", "on", "group_left", "group_right", "quantile", "stddev", "and",
    "or", "unless", "le", "offset", "bool", "time", "vector", "scalar", "absent",
}


RGBA_RE = re.compile(r"rgba\(\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)\s*(?:,\s*[\d.]+\s*)?\)")


def rewrite_colors(obj):
    """perses CUE schemas only accept hex colors (#rgb / #rrggbb); convert rgba()."""
    if isinstance(obj, dict):
        return {k: rewrite_colors(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [rewrite_colors(v) for v in obj]
    if isinstance(obj, str):
        return RGBA_RE.sub(
            lambda m: "#{:02x}{:02x}{:02x}".format(*[int(m.group(i)) for i in (1, 2, 3)]), obj
        )
    return obj


def rewrite_expr(expr: str) -> str:    # metric renames (longest first so persec renames don't double-apply)
    for old in sorted(METRIC_RENAMES, key=len, reverse=True):
        expr = re.sub(r"\b%s\b" % re.escape(old), METRIC_RENAMES[old], expr)
    # label renames (word-boundary, keeps $var names intact)
    for old, new in LABEL_MAP.items():
        expr = re.sub(r"(?<![\w$])%s(?![\w])" % re.escape(old), new, expr)
    # $kubernetes_namespace -> $namespace (var rename)
    expr = re.sub(r"\$kubernetes_namespace", "$namespace", expr)
    # pod-name matcher: kubernetes_pod_name=~"$cluster-$broker" -> pod=~".*$broker"
    expr = re.sub(r'\bpod=~"\$[a-z_]+-\$([a-z_]+)"', r'pod=~".*$\1"', expr)
    # exact matchers on variable values -> regex matchers (multi/all safe)
    expr = re.sub(r'([a-zA-Z_][a-zA-Z_0-9]*)="(\$[a-zA-Z_][a-zA-Z_0-9]*)"', r'\1=~"\2"', expr)
    # Grafana magic interval -> fixed 5m
    expr = expr.replace("__rate_interval", "5m")
    return expr


def expr_metric_names(expr: str) -> set:
    toks = set(re.findall(r"[a-zA-Z_:][a-zA-Z0-9_:]*", expr))
    return {t for t in toks if not t.startswith("$") and t not in IGNORE_TOKENS}


def is_unavailable(expr: str) -> bool:
    names = expr_metric_names(expr)
    return bool(names) and all(
        n.startswith(UNAVAILABLE_PREFIXES) for n in names
    )


def grafana_panels(gdash):
    """Yield non-row Grafana panels in document order."""
    for p in gdash.get("panels", []):
        if p.get("type") == "row":
            if p.get("collapsed"):
                yield from grafana_panels({"panels": p.get("panels", [])})
        else:
            yield p


def convert(name, grafana_file, migrated_file):
    gdash = json.load(open(grafana_file))
    migrated = json.load(open(migrated_file))
    mspec = migrated["spec"]

    # map grafana panel titles (in order) to targets
    queues = {}
    for gp in grafana_panels(gdash):
        queues.setdefault(gp.get("title"), []).append(gp)

    dropped = []
    dropped_keys = set()
    panels_out = {}
    for grid in mspec.get("layouts", []):
        for item in grid["spec"]["items"]:
            ref = item.get("content", {}).get("$ref")
            if not ref:
                continue
            key = ref.split("/")[-1]
            panel = rewrite_colors(mspec["panels"][key])
            title = panel["spec"].get("display", {}).get("name")
            gq = queues.get(title, [])
            gp = gq.pop(0) if gq else None
            targets = gp.get("targets", []) if gp else []
            exprs = [t.get("expr", "") for t in targets if t.get("expr")]
            exprs = [e for e in exprs if e.strip()]
            if not exprs:
                panels_out[key] = panel  # text/markdown panel, keep as migrated
                continue
            if all(is_unavailable(e) for e in exprs):
                dropped.append(title)
                dropped_keys.add(key)
                continue
            plugin_kind = panel["spec"]["plugin"]["kind"]
            if plugin_kind not in ("TimeSeriesChart", "StatChart", "BarChart", "Table"):
                plugin_kind = "TimeSeriesChart"
            queries = [
                {
                    "kind": "TimeSeriesQuery",
                    "spec": {
                        "plugin": {
                            "kind": "PrometheusTimeSeriesQuery",
                            "spec": {"datasource": DATASOURCE, "query": rewrite_expr(e)},
                        }
                    },
                }
                for e in exprs
            ]
            panels_out[key] = {
                "kind": "Panel",
                "spec": {
                    "display": panel["spec"].get("display", {"name": title}),
                    "plugin": panel["spec"]["plugin"],
                    "queries": queries,
                },
            }

    # keep layouts, minus items whose panels were dropped
    layouts = []
    for grid in mspec.get("layouts", []):
        items = [
            it for it in grid["spec"].get("items", [])
            if it.get("content", {}).get("$ref", "").split("/")[-1] not in dropped_keys
        ]
        if items:
            g = json.loads(json.dumps(grid))
            g["spec"]["items"] = items
            layouts.append(g)

    # variables from the Grafana templating (drop the datasource variable)
    variables = []
    for t in gdash.get("templating", {}).get("list", []):
        vname = t.get("name")
        if vname == "DS_PROMETHEUS" or t.get("type") == "datasource":
            continue
        vname = VAR_NAME.get(vname, vname)
        # representative metric from the var's query for the matchers
        q = t.get("query")
        if isinstance(q, dict):
            q = q.get("query", "")
        m = re.search(r"\(([a-zA-Z_:][a-zA-Z0-9_:]*)", str(q))
        metric = m.group(1) if m else "up"
        metric = rewrite_expr(metric)
        label = VAR_LABEL.get(vname, vname)
        allow_all = bool(t.get("includeAll"))
        variables.append(
            {
                "kind": "ListVariable",
                "spec": {
                    "name": vname,
                    "allowMultiple": bool(t.get("multi")),
                    "allowAllValue": allow_all,
                    "display": {"name": t.get("label", vname), "hidden": False},
                    "plugin": {
                        "kind": "PrometheusLabelValuesVariable",
                        "spec": {
                            "datasource": DATASOURCE,
                            "labelName": label,
                            "matchers": [metric],
                        },
                    },
                },
            }
        )
        if allow_all:
            variables[-1]["spec"]["customAllValue"] = t.get("allValue", ".*")

    dash = {
        "apiVersion": "perses.dev/v1alpha2",        "kind": "PersesDashboard",
        "metadata": {"name": name, "namespace": "kafka"},
        "spec": {
            "instanceSelector": {
                "matchLabels": {"app.kubernetes.io/instance": "perses-observability-operator"}
            },
            "config": {
                "display": {
                    "name": gdash.get("title", name),
                    "description": (
                        f"Converted from the Strimzi Grafana dashboard "
                        f"({name}) to Perses. Metrics use Kafka 4.x naming, verified "
                        f"against the live cluster; queries target the "
                        f"user-workload-monitoring datasource (Thanos Querier)."
                    ),
                },
                "duration": "6h",
                "refreshInterval": "1m",
                "variables": variables,
                "layouts": layouts,
                "panels": panels_out,
            },
        },
    }
    return dash, dropped


if __name__ == "__main__":
    class NoAliasDumper(yaml.SafeDumper):
        def ignore_aliases(self, data):
            return True

    # quote scalars that start with a digit (plain "0_8" is parsed as the
    # number 8 by kubectl's YAML parser, breaking the panel $refs)
    def _str_repr(dumper, data):
        if re.match(r"^\d", data):
            return dumper.represent_scalar("tag:yaml.org,2002:str", data, style="'")
        return dumper.represent_scalar("tag:yaml.org,2002:str", data)

    NoAliasDumper.add_representer(str, _str_repr)

    for name in sys.argv[1:]:
        dash, dropped = convert(
            name, f"grafana-json/{name}.json", f"online-json/{name}.json"
        )
        with open(f"converted/{name}.yaml", "w") as fh:
            yaml.dump(dash, fh, Dumper=NoAliasDumper, sort_keys=False, default_flow_style=False, width=200)
        print(f"{name}: {len(dash['spec']['config']['panels'])} panels, "
              f"{len(dash['spec']['config']['variables'])} variables, dropped: {dropped or 'none'}")
