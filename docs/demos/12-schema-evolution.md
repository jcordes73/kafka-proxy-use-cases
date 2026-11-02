# Demo 12: Schema evolution

> [↑ Demo scenarios](../11-demos.md) · prev: [Demo 11: Record Encryption at rest](11-record-encryption.md) · next: [Demo 13: Custom filter](13-custom-filter.md)

Extends Demo 2:

1. Produce a valid record (accepted).
2. Change the JSON schema in Apicurio Registry (UI or REST — e.g. make a field required).
3. Produce the same record again — now rejected with `Broker failed to validate record`.
4. The contract changed **once, centrally** — no client redeployment, no broker config.
