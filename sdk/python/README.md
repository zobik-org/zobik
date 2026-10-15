# sdk/python

The Python client library of the Node Runtime Interface (implementation §1.2.13). Optional: any image that satisfies the interface is a valid Logic Container.

* `src/zobik` is the library, and `src/zobik/nri/v1` the stubs generated from `proto/zobik/nri/v1` by `generate.sh`, with the tools pinned in `requirements-codegen.txt`.
* `requirements.txt` pins the library's runtime dependencies, which the base image (`images/zobik-python.Dockerfile`) installs.
* The tests run against a fake Integration Sidecar: `PYTHONPATH=src python -m unittest discover -s tests`.
