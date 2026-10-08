# Zobik

A decentralized network of AI nodes with no master controller.

Zobik is an event-driven Compound AI System built on the Blackboard and Pub/Sub patterns. Nodes select themselves by topic subscription and semantic similarity, compete for the work published on an event bus, and the network grows new capabilities without changes to the system. No component knows the others by name.

A Zobik network runs on a single machine: the person's own computer.

**Status: early development.** There is nothing to install yet.

## Repository layout

| Path | Contents |
| :--- | :--- |
| `cmd/zobik` | The `zobik` binary: the operator console, the command line and every structural role. |
| `cmd/zobik-sidecar` | The Integration Sidecar, the generic half of every node. |
| `internal/` | The Go packages behind both binaries, with one package per role under `internal/role/`. |
| `proto/` | The Node Runtime Interface between the Integration Sidecar and the Logic Container. |
| `schemas/` | The JSON Schemas shipped with each release. |
| `sdk/python/` | The Python client library of the Node Runtime Interface. |
| `images/` | Dockerfiles for the roles, the sidecar and the Logic Container base image. |
| `bundles/forge/` | The Forge starter bundle. |
| `web/` | The operator panel. |
| `packaging/` | Native installation packages. |

## Author

Zobik is designed and developed by Rodrigo Díaz Velasco ([@theviderlab](https://github.com/theviderlab)).

## License

Apache License 2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).

## Security

See [SECURITY.md](SECURITY.md).
