# The echo blueprint's Logic Container, on the base image.
# Build from the repository root, after the base image:
#   docker build -f examples/blueprints/echo/Dockerfile -t zobik-echo:dev .
ARG BASE=zobik-python:dev
FROM ${BASE}
COPY examples/blueprints/echo/echo.py /opt/echo/echo.py
# Readable by the UID the Spawner sets (implementation §2.4).
RUN chmod -R a+rX /opt/echo
CMD ["python", "/opt/echo/echo.py"]
