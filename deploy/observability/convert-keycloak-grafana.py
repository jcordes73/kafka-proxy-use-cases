#!/usr/bin/env python3
"""Convert Keycloak Grafana dashboards (keycloak/keycloak-grafana-dashboard) to
Perses v1alpha2 PersesDashboard CRs.

Self-contained (no percli): reads the Grafana JSON(s) directly and emits CRs in
the same verified shape as deploy/observability/09-*/10-*:
  - Grafana rows -> Grid layouts with title + display.collapse {open: true}
    (the verified-live layout pattern: bare titles render with broken panel
    positions; y-coordinates continue across grids)
  - timeseries -> TimeSeriesChart, gauge/stat -> StatChart
  - queries: datasource fixed to the `user-workload-monitoring` Perses
    datasource; $__rate_interval -> 5m; $__range -> 6h (the dashboard duration);
    exact matchers on $vars -> regex matchers (multi/all safe)
  - target legendFormat -> seriesNameFormat ({{label}} templates map 1:1), which
    keeps multi-target panels from rendering identical legend entries/colors
  - variables: only those actually used in queries are kept (namespace); unused
    Grafana vars are dropped
  - ALL panels are kept, including those using metrics this Keycloak build does
    not expose (keycloak_user_events_total) — they populate if/when the metric
    appears (same precedent as the 09 Strimzi dashboards' undeployed components)

Usage: convert-keycloak-grafana.py <dashboard.json> [<more.json> ...]
Writes converted/<name>.yaml — one PersesDashboard CR per input file.
"""
import json
import os
import re
import sys

import yaml

DATASOURCE = {"kind": "PrometheusDatasource", "name": "user-workload-monitoring"}

# Grafana unit -> Perses TimeSeriesChart yAxis format unit ('short' has no
# Perses equivalent — omit the format entirely, verified against 07/09/10)
UNIT_MAP = {
    "short": None,
    "none": None,
    "percent": "percent",
    "percentunit": "percent-decimal",
    "bytes": "bytes",
    "decbytes": "bytes",
    "s": "seconds",
    "ms": "milliseconds",
    "reqps": "requests/sec",
    "ops": "requests/sec",
    "pps": "decimal",
}

# Grafana named colors -> hex (Perses CUE schemas only accept hex)
COLOR_MAP = {
    "green": "#73bf69", "dark-green": "#73bf69", "super-light-green": "#96d98d",
    "yellow": "#fade2a", "orange": "#ff9830", "dark-orange": "#ff9830",
    "red": "#f2495c", "dark-red": "#f2495c", "blue": "#5794f2",
    "text": "#ccccdc",
}


def grafana_color(color):
    c = COLOR_MAP.get(color)
    if c:
        return c
    if isinstance(color, str) and color.startswith("#"):
        return color
    return "#ccccdc"


def scale(value):
    """Grafana grid units -> Perses grid units (h in 30px units both, we scale
    rows slightly tighter to match the 09/10 look)."""
    return max(int(round(value * 0.7)), 4)


def rewrite_expr(expr):
    # Grafana magic intervals -> fixed windows (dashboard duration is 6h)
    expr = expr.replace("[$__range:$__interval]", "[6h:5m]")
    expr = expr.replace("[$__range]", "[6h]")
    expr = re.sub(r"\$__rate_interval", "5m", expr)
    expr = re.sub(r"\$__interval", "5m", expr)
    # exact matchers on variable values -> regex matchers (multi/all safe)
    expr = re.sub(r'([a-zA-Z_][a-zA-Z_0-9]*)="(\$[a-zA-Z_][a-zA-Z_0-9]*)"', r'\1=~"\2"', expr)
    # collapse whitespace from the Grafana multi-line formatting
    expr = re.sub(r"\s+", " ", expr).strip()
    return expr


def expr_metric_names(expr):
    ignore = {
        "rate", "irate", "sum", "by", "avg", "max", "min", "increase", "count",
        "count_over_time", "avg_over_time", "max_over_time", "min_over_time",
        "topk", "bottomk", "histogram_quantile", "label_replace", "without",
        "on", "group_left", "group_right", "quantile", "and", "or", "unless",
        "le", "offset", "bool", "time", "vector", "scalar", "absent", "up",
    }
    toks = set(re.findall(r"[a-zA-Z_:][a-zA-Z0-9_:]*", expr))
    return {t for t in toks if not t.startswith("$") and t not in ignore}


def series_name_format(legend_format):
    """Grafana legendFormat -> Perses seriesNameFormat ('__auto'/empty -> None)."""
    if not legend_format or legend_format.strip() in ("__auto", ""):
        return None
    return legend_format.strip()


def convert_panel(p):
    """Grafana panel -> Perses Panel (or None to skip)."""
    ptype = p.get("type", "")
    fc = p.get("fieldConfig", {}).get("defaults", {})
    unit = UNIT_MAP.get(fc.get("unit"), None)
    display = {"name": p.get("title", ""), "description": (p.get("description") or "")[:200] or None}
    display = {k: v for k, v in display.items() if v}

    targets = [t for t in p.get("targets", []) if (t.get("expr") or "").strip()]
    queries = []
    for t in targets:
        qspec = {"datasource": DATASOURCE, "query": rewrite_expr(t["expr"])}
        snf = series_name_format(t.get("legendFormat", ""))
        if snf:
            qspec["seriesNameFormat"] = snf
        queries.append({
            "kind": "TimeSeriesQuery",
            "spec": {"plugin": {"kind": "PrometheusTimeSeriesQuery", "spec": qspec}},
        })

    if ptype in ("timeseries", "time_series", "graph"):
        ts = {"legend": {"mode": "list", "position": "bottom"},
              "visual": {"areaOpacity": 0.1, "connectNulls": False, "display": "line", "lineWidth": 1}}
        if unit:
            ts["yAxis"] = {"format": {"unit": unit}}
        return {"kind": "Panel", "spec": {"display": display, "plugin": {"kind": "TimeSeriesChart", "spec": ts}, "queries": queries}}

    if ptype in ("gauge", "stat", "stattext", "singlestat"):
        st = {"calculation": "last-number", "colorMode": "value"}
        if unit:
            st["format"] = {"unit": unit}
        steps = (fc.get("thresholds") or {}).get("steps")
        if steps:
            st["thresholds"] = {"steps": [
                {"color": grafana_color(s.get("color")), "value": s.get("value", 0)} for s in steps
            ]}
        return {"kind": "Panel", "spec": {"display": display, "plugin": {"kind": "StatChart", "spec": st}, "queries": queries}}

    return None  # text/table/etc: skipped


def convert(name, path):
    gd = json.load(open(path))

    grids = []           # [{"title": str, "items": [...]}] with grid-relative y
    current = None
    panels_out = {}
    key_n = 0

    def add_panel(p):
        """Convert one Grafana panel and append to the current grid."""
        nonlocal key_n
        panel = convert_panel(p)
        if panel is None:
            return
        key = str(key_n)
        key_n += 1
        panels_out[key] = panel
        gp = p.get("gridPos", {})
        current["items"].append({
            "x": gp.get("x", 0), "y": scale(gp.get("y", 0)),
            "width": gp.get("w", 12), "height": scale(gp.get("h", 8)),
            "content": {"$ref": "#/spec/panels/%s" % key},
        })

    def flush():
        nonlocal current
        if current and current["items"]:
            grids.append(current)
        current = None

    for p in gd.get("panels", []):
        if p.get("type") == "row":
            flush()
            current = {"title": p.get("title", ""), "items": []}
            # collapsed Grafana rows nest their panels inside the row —
            # include them (gridPos.y is relative to the row)
            for nested in p.get("panels", []):
                add_panel(nested)
            continue
        if current is None:
            current = {"title": "", "items": []}
        add_panel(p)
    flush()

    # rebase each grid's y to continue across grids (absolute, non-overlapping)
    layouts = []
    y_cursor = 0
    for g in grids:
        min_y = min(it["y"] for it in g["items"])
        for it in g["items"]:
            it["y"] = it["y"] - min_y
        # resolve overlaps (Grafana gridPos can overlap): push panels down
        placed = []
        for it in sorted(g["items"], key=lambda i: (i["y"], i["x"])):
            y = it["y"]
            moved = True
            while moved:
                moved = False
                for p in placed:
                    if (it["x"] < p["x"] + p["width"] and p["x"] < it["x"] + it["width"]
                            and y < p["y"] + p["height"] and p["y"] < y + it["height"]):
                        y = p["y"] + p["height"]
                        moved = True
            it["y"] = y
            placed.append(it)
        row_h = max(it["y"] + it["height"] for it in g["items"])
        items = [{
            "x": it["x"], "y": y_cursor + it["y"],
            "width": it["width"], "height": it["height"],
            "content": it["content"],
        } for it in g["items"]]
        disp = {"title": g["title"] or "Panels", "collapse": {"open": True}}
        layouts.append({"kind": "Grid", "spec": {"display": disp, "items": items}})
        y_cursor += row_h

    # variables actually used in queries
    used_vars = set()
    for panel in panels_out.values():
        for q in panel["spec"].get("queries", []):
            used_vars |= set(re.findall(r"\$([a-zA-Z_][a-zA-Z_0-9]*)", q["spec"]["plugin"]["spec"]["query"]))
    # var -> Prometheus label it matches + a representative metric carrying it
    VAR_SPEC = {
        "namespace": ("namespace", "up"),
        "realm": ("realm", "keycloak_credentials_password_hashing_validations_total"),
        "jdbc_cache_names": ("cache", "cache_evictions_count"),
        "caffeine_cache": ("cache", "cache_evictions_count"),
        "ispn_local_cache": ("cache", "vendor_statistics_hits"),
        "ispn_clustered_caches": ("cache", "vendor_rpc_manager_replication_count"),
    }
    variables = []
    for vname in sorted(used_vars):
        label, metric = VAR_SPEC.get(vname, (vname, "up"))
        variables.append({
            "kind": "ListVariable",
            "spec": {
                "name": vname,
                "allowMultiple": vname != "namespace",
                "allowAllValue": True,
                "display": {"name": label, "hidden": False},
                "plugin": {
                    "kind": "PrometheusLabelValuesVariable",
                    "spec": {"datasource": DATASOURCE, "labelName": label, "matchers": [metric]},
                },
                "customAllValue": ".*",
            },
        })

    dash = {
        "apiVersion": "perses.dev/v1alpha2",
        "kind": "PersesDashboard",
        "metadata": {"name": name, "namespace": "kafka"},
        "spec": {
            "instanceSelector": {"matchLabels": {"app.kubernetes.io/instance": "perses-observability-operator"}},
            "config": {
                "display": {
                    "name": gd.get("title", name),
                    "description": (
                        "Converted from the Keycloak Grafana dashboard "
                        "(keycloak/keycloak-grafana-dashboard) to Perses. Scraped from the "
                        "Red Hat build of Keycloak management port (9000, HTTPS); queries "
                        "target the user-workload-monitoring datasource (Thanos Querier)."
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
    return dash


if __name__ == "__main__":
    class NoAliasDumper(yaml.SafeDumper):
        def ignore_aliases(self, data):
            return True

    os.makedirs("converted", exist_ok=True)
    for path in sys.argv[1:]:
        name = os.path.basename(path).replace("-dashboard.json", "").replace(".json", "")
        dash = convert(name, path)
        out = f"converted/{name}.yaml"
        with open(out, "w") as fh:
            yaml.dump(dash, fh, Dumper=NoAliasDumper, sort_keys=False, default_flow_style=False, width=200)
        print(f"{name}: {len(dash['spec']['config']['panels'])} panels, "
              f"{len(dash['spec']['config']['layouts'])} grids, "
              f"{len(dash['spec']['config']['variables'])} variables -> {out}")
