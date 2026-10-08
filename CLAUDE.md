# Zobik

Implementation of the Zobik network. The design is specified in documents that live outside this repository; code implements them and does not restate them.

## Conventions

* Everything in this repository is written in English: code, comments, commit messages, issues and docs.
* A comment that implements a clause cites it by document and section: `// architecture §3.1`, `// implementation §6`.
* System identifiers are the ones in the specification, in `snake_case`: role names, configuration keys, payload fields, topics and event types (`task.announced`). Go package names drop the underscore (`taskbroker` for the `task_broker` role).
* "Node", never "agent", names a component of the network.
* The Go module is `zobik.org/zobik`. Nothing names the GitHub organization.
* Each development stage ends with an end-to-end test in `test/e2e/` that demonstrates it.
* No insecure mode: every piece enters with its full contract, signatures and validation included. Credentials a component does not issue yet are minted by the test harness with the same root.
