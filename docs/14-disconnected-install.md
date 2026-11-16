# Reference: Preparing a disconnected (air-gapped) installation

> [↑ Setup overview](../openshift-setup.md) · prev: [Step 12: Cleanup](12-cleanup.md)

This document prepares this stack's deployment for an **air-gapped OpenShift cluster**: a private registry holds the operator catalogs and every image the operators and manifests reference, the six OLM subscriptions point at the mirrored catalog, and the pinned CSVs ([Components and Tested Versions](../README.md#components-and-tested-versions)) decide what installs — same as connected, just sourced from your mirror.

An air-gapped install has **three layers of images** to mirror, plus the subscription repointing:

| Layer | What | Where it comes from |
|---|---|---|
| 1. OLM catalogs + operator bundles | The six operators this stack subscribes to | `registry.redhat.io/redhat/redhat-operator-index:v4.22` via `oc mirror` |
| 2. Operand images | What the operators deploy **at runtime** (`registry.redhat.io/...` — referenced by the CSVs, *not* contained in the catalog) | enumerated from the pinned CSVs (`relatedImages`) |
| 3. Images referenced directly by this repo's manifests | a few non-Red-Hat ones — flagged in [Step 5](#step-5--the-directly-referenced-non-red-hat-images) | `docker.io` + Maven |

**Nothing in the *data path* needs cluster egress** — Keycloak JWKS/token traffic flows through the in-cluster `keycloak-http` Service ([Step 2.3](02-keycloak.md#23--tls-certificate-and-keycloak-deployment)), and the proxy, bridge, and MM2 all talk in-cluster. Images are the only external dependency.

Prerequisites: `cluster-admin`, an internal registry with push access reachable from both the connected host (for mirroring) and the cluster (for pulls), and the `oc-mirror` v2 plugin on the connected host (bundled with `oc`; check the [disconnected installation docs](https://docs.redhat.com/en/documentation/openshift_container_platform/) for your OpenShift version's exact command shape).

## Step 1 — Enumerate the image set (whitelisting input)

Run on a **connected** cluster (or against the catalog in your connected build environment) after installing the pinned CSVs — the operators' `relatedImages` field is the authoritative list of operator + operand images:

```bash
# Every image (operator + operand) of every installed CSV, sorted and deduped
oc get csv -A -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{range .spec.relatedImages[*]}  {.name}={.image}{"\n"}{end}{end}' | sort -u
```

For this stack that yields (all `registry.redhat.io`): the operator images (`amq-streams-rhel9-operator`, the proxy/console/keycloak/apicurio operators) and operands `kafka-42-rhel9` (Kafka 4.2.2), `bridge-rhel9`, the proxy operand, `console-api-rhel9`/`console-ui-rhel9`, the Keycloak server image, and the Apicurio Registry images. **Enumerate — don't hand-maintain**: the CSV pins in this repo guarantee the list matches what actually installs.

## Step 2 — Mirror the operator catalogs (`oc mirror` v2)

```yaml
# imageset-config.yaml — the six operators this stack subscribes to
kind: ImageSetConfiguration
apiVersion: mirror.openshift.io/v2alpha1
mirror:
  operators:
    - catalog: registry.redhat.io/redhat/redhat-operator-index:v4.22
      packages:
        - name: amq-streams                    # Kafka clusters (channel amq-streams-3.2.x)
        - name: amq-streams-proxy              # Proxy operator (channel amq-streams-3.2.x)
        - name: amq-streams-console            # Console operator (channel amq-streams-3.2.x)
        - name: rhbk-operator                  # Keycloak (channel stable-v26.4)
        - name: apicurio-registry-3            # Apicurio Registry (channel 3.2.x)
        - name: cluster-observability-operator # optional — Perses dashboards (channel stable)
```

```bash
# On the connected host, mirror into the internal registry
oc mirror --v2 --config imageset-config.yaml docker://registry.internal.example.com/olm
```

This produces the workspace artifacts — a **`CatalogSource`** and the mirror **`ImageDigestMirrorSet`** — apply them:

```bash
oc apply -f oc-mirror-workspace/results-*/catalogSource-*.yaml
oc apply -f oc-mirror-workspace/results-*/imageDigestMirrorSet-*.yaml
```

Mirroring whole channels is the safe default; the Subscriptions' `startingCSV` pins ([deploy/README.md](../deploy/README.md)) still decide what actually installs. `installPlanApproval: Manual` prevents OLM from auto-upgrading to anything newer that the mirrored channel carries.

## Step 3 — Repoint the six Subscriptions at the mirrored catalog

The `CatalogSource` lands in `openshift-marketplace` with a generated name (see the applied artifact) — repoint every `source: redhat-operators` in this repo:

```bash
sed -i 's/source: redhat-operators/source: <catalogsource-name>/' \
  deploy/kafka/00-cluster-operator-olm.yaml \
  deploy/keycloak/01-kafka-auth-operator.yaml \
  deploy/proxy/00-proxy-operator-olm.yaml \
  deploy/console/01-console-operator.yaml \
  deploy/apicurio/01-subscription.yaml \
  deploy/observability/06-perses.yaml
```

(`sourceNamespace: openshift-marketplace` stays; `installPlanApproval: Manual` + `startingCSV` keep the pinned versions — the CSVs now come from your mirror.)

## Step 4 — Mirror the operand images and map `registry.redhat.io`

Operand images are pulled at **runtime** by the operators from `registry.redhat.io` — mirror them from the Step 1 enumeration into the internal registry (e.g., a `skopeo copy` loop over the enumeration output, or a second `oc mirror` imageset), then map the source registry so runtime pulls resolve:

```yaml
apiVersion: config.openshift.io/v1
kind: ImageDigestMirrorSet
metadata:
  name: redhat-to-mirror
spec:
  imageDigestMirrors:
    - source: registry.redhat.io
      mirrors:
        - registry.internal.example.com/redhat
```

> Operand images pinned by **tag** (not digest) need an `ImageTagMirrorSet` alongside the digest mirror set — Red Hat operator operands are digest-pinned, but verify per CSV from the Step 1 enumeration output.

## Step 5 — The directly-referenced (non-Red-Hat) images

This repo's manifests and `oc run` verification commands pull five images from outside `registry.redhat.io` — mirror them and either edit the manifests or add an `ImageTagMirrorSet` for `docker.io`:

| Image | Used by | Manifests |
|---|---|---|
| `docker.io/library/postgres:15` | Keycloak + Apicurio Registry databases | `deploy/keycloak/02-postgres.yaml`, `deploy/apicurio/02-postgres.yaml` |
| `docker.io/curlimages/curl` | the `oc run` test pods throughout the docs | (commands, not manifests) |
| `quay.io/strimzi/kafka:latest-kafka-4.2.2` | the Kafka CLI pods ([6.10](06-proxy.md#610--apply-and-verify), [Demo 5](demos/05-stable-endpoints.md)) and the Kafka-channel load-test Jobs ([15](15-load-testing.md)) | `deploy/loadtest/01–02` |
| `docker.io/grafana/k6` | the bridge-channel load-test Jobs ([15](15-load-testing.md)) — pin the tag for reproducibility | `deploy/loadtest/03–04` |
| Camel/Quarkus client images | built via Maven `BuildConfig`s ([Step 9](09-camel-clients.md)) | point the builds at an internal Maven mirror (Artifactory/Nexus `settings.xml`), or build on a connected host and push the resulting images |

## Step 6 — Verify

```bash
oc get catalogsource -n openshift-marketplace           # mirrored catalog READY
oc get packagemanifests -n kafka amq-streams            # visible from the watched namespace
oc get subscriptions -A -o jsonpath='{range .items[*]}{.metadata.namespace}/{.metadata.name}{"\t"}{.spec.source}{"\n"}{end}'
# deploy the stack (deploy/README.md deploy order) — every pod should pull
# from registry.internal.example.com; watch for ImagePullBackOff as the tell
```

## Notes

- **Proxy Technology Preview caveat unchanged:** air-gapping changes image sourcing, not support posture — the proxy is still TP in Streams 3.2, and the [compatibility vs supportability note](../README.md#components-and-tested-versions) applies to any non-Red-Hat backend the PoC connects to.
- **Upgrade discipline:** when advancing a pinned CSV, re-run Steps 1–4 — new CSVs bring new operand images that must exist in the mirror before the InstallPlan is approved.
- **Test pods:** replace `curlimages/curl` in the docs' `oc run` verification commands with the mirrored path (or a mirrored `registry.redhat.io/ubi9` + `microdnf install curl` alternative) once `docker.io` is unreachable.
- **OpenShift version pinning:** mirror the operator index **tag matching the cluster's OCP version** (`v4.22` here) — a mismatched index can advertise CSVs the cluster's OLM can't install.
