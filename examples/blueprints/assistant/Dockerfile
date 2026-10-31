# The assistant blueprint's Logic Container, on the base image, with LangGraph
# and the OpenAI variant's model adapter.
# Build from the repository root, on the base image zobik init seeded in the
# tenant's registry (implementation §1.2.13):
#   docker build -f examples/blueprints/assistant/Dockerfile -t zobik-assistant:dev \
#     --build-arg BASE=$(zobik base-image) .
ARG BASE=zobik-python:dev
FROM ${BASE}
COPY bundles/forge/openai/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir --require-hashes -r /tmp/requirements.txt && rm /tmp/requirements.txt
COPY bundles/forge/openai/zobik_openai.py /opt/assistant/zobik_openai.py
COPY examples/blueprints/assistant/assistant.py /opt/assistant/assistant.py
# Readable by the UID the Spawner sets, and compiled ahead because the image's
# filesystem is read-only (implementation §2.4).
RUN chmod -R a+rX /opt/assistant && python -m compileall -q /opt/assistant
CMD ["python", "/opt/assistant/assistant.py"]
