# examples/blueprints

Blueprints used to exercise each development stage.

| Blueprint | What it does |
| :--- | :--- |
| [echo](echo) | Writes the `task_text` of its task to the portion `result` of its section and completes with its `context_ref`. A `task_text` that starts with `?` is first asked to whoever originated the trace, with the options `yes` and `no`, and the answer goes after the text. |
| [historian](historian) | Announces `trace_query` on its own trace, writes the reconstruction to the portion `result` of its section, prints it to its log and completes with its `context_ref`. |
