# The historian blueprint's Logic Container, on the base image.
# Build from the repository root, after the base image:
#   docker build -f examples/blueprints/historian/Dockerfile -t zobik-historian:dev .
ARG BASE=zobik-python:dev
FROM ${BASE}
COPY examples/blueprints/historian/historian.py /opt/historian/historian.py
# Readable by the UID the Spawner sets (implementation §2.4).
RUN chmod -R a+rX /opt/historian
CMD ["python", "/opt/historian/historian.py"]
