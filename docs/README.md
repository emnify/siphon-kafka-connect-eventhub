# Kafka Connect Azure Event Hubs — documentation

This documentation follows the [Diátaxis](https://diataxis.fr/) framework. Four kinds of document, each answering a
different question, kept deliberately separate.

|                     | Practical steps                                     | Theoretical knowledge                                       |
|---------------------|-----------------------------------------------------|-------------------------------------------------------------|
| **Serving study**   | [Tutorials](#tutorials) — *learn by doing*           | [Explanation](#explanation) — *understand why*               |
| **Serving work**    | [How-to guides](#how-to-guides) — *solve a problem*  | [Reference](#reference) — *look up a fact*                   |

## Tutorials

Start here if you have never run this connector.

- [Stream your first topic to an Event Hub](tutorials/01-first-sink-connector.md)

## How-to guides

Recipes for a specific goal, assuming you already know the basics.

- [Build and deploy the connector](how-to/build-and-deploy.md)
- [Authenticate with a SAS connection string](how-to/authenticate-with-sas.md)
- [Authenticate with a filesystem JWT](how-to/authenticate-with-filesystem-jwt.md)
- [Survive intermittent Event Hubs errors](how-to/handle-intermittent-errors.md)
- [Dead letter poison records](how-to/dead-letter-poison-records.md)
- [Preserve per-key ordering](how-to/preserve-record-ordering.md)
- [Tune throughput](how-to/tune-throughput.md)
- [Connect through a firewall or proxy](how-to/connect-through-a-firewall.md)
- [Scan dependencies for known CVEs](how-to/scan-dependencies-for-cves.md)
- [Upgrade from the v3 SDK build](how-to/upgrade-from-v3.md)

## Reference

- [Configuration properties](reference/configuration.md)
- [Record to event mapping](reference/data-mapping.md)
- [Error classification table](reference/error-classification.md)
- [Logging](reference/logging.md)

## Explanation

- [Architecture](explanation/architecture.md)
- [Fail fast or retry](explanation/fail-fast-vs-retry.md)
- [The two-layer retry model](explanation/retry-model.md)
- [Delivery semantics](explanation/delivery-semantics.md)

## Related

- [Improvement backlog](../IMPROVEMENTS.md) — reviewed findings not yet addressed.
