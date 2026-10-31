# The historian blueprint's Logic Container, on the base image.
# Build from the repository root, on the base image zobik init seeded in the
# tenant's registry (implementation §1.2.13):
#   docker build -f examples/blueprints/historian/Dockerfile -t zobik-historian:dev \
#     --build-arg BASE=$(zobik base-image) .
ARG BASE=zobik-python:dev
FROM ${BASE}
COPY examples/blueprints/historian/historian.py /opt/historian/historian.py
# Readable by the UID the Spawner sets (implementation §2.4).
RUN chmod -R a+rX /opt/historian
CMD ["python", "/opt/historian/historian.py"]
