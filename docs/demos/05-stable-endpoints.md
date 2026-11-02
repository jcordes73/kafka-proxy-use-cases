# Demo 5: Stable endpoints

> [↑ Demo scenarios](../10-demos.md) · prev: [Demo 4: DMZ pivot attempt](04-dmz-pivot.md) · next: [Demo 6: Authorization at the proxy](06-proxy-authorization.md)

Clients never see broker topology changes:

```bash
# Roll every broker of the cluster the proxy targets
oc rollout restart deployment -n kafka-cluster-1 || oc delete pod -l strimzi.io/cluster=kafka-cluster-1 -n kafka-cluster-1

# The Camel producer (timer-driven) keeps producing through the proxy with no
# errors during the roll — its bootstrap is the proxy's clusterIP bootstrap
oc logs -f deployment/camel-kafka-producer -n kafka-producer
```
