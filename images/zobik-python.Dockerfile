# The Logic Container base image: Python with the NRI client library (implementation §1.2.13).
# Build from the repository root: docker build -f images/zobik-python.Dockerfile -t zobik-python:dev .
FROM python:3.12-slim@sha256:a0f57064cbdcd6285fa45f51237ab0b35ea8ebaed06a1f5973e89e19dfb86704
COPY sdk/python/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir --require-hashes -r /tmp/requirements.txt && rm /tmp/requirements.txt
COPY sdk/python/src/zobik /usr/local/lib/python3.12/site-packages/zobik
# Readable by the UID the Spawner sets, and compiled ahead because the image's
# filesystem is read-only (implementation §2.4).
RUN chmod -R a+rX /usr/local/lib/python3.12/site-packages/zobik \
 && python -m compileall -q /usr/local/lib/python3.12/site-packages/zobik
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
# The version the library sends in its Hello: zobik.NRI_VERSION, which the
# library's tests compare with this label (implementation §1.2.13).
LABEL io.zobik.nri.version="1.0"
