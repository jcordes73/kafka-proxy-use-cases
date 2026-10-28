#!/usr/bin/env bash
# deploy/env.sh — cluster-specific environment for manifest templating.
#
# Source this before applying manifests that contain ${OPENSHIFT_APPS_DOMAIN}:
#
#   source deploy/env.sh
#   envsubst < deploy/kafka/02-kafka.yaml | oc apply -f -
#
# OPENSHIFT_APPS_DOMAIN is the cluster's default Ingress Controller domain —
# the suffix of every Route hostname (e.g. kafka-console.<domain>).
export OPENSHIFT_APPS_DOMAIN="$(
  oc get ingresscontrollers.operator.openshift.io -n openshift-ingress-operator default \
    -o jsonpath='{.status.domain}' 2>/dev/null
)"
if [ -z "$OPENSHIFT_APPS_DOMAIN" ]; then
  echo "ERROR: could not discover the cluster ingress domain (are you logged in with oc?)" >&2
  return 1 2>/dev/null || exit 1
fi
echo "OPENSHIFT_APPS_DOMAIN=${OPENSHIFT_APPS_DOMAIN}"
