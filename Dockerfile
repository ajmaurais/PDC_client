from python:3.9-slim

MAINTAINER "Aaron Maurais -- MacCoss Lab"

RUN apt-get update && \
    apt-get -y install procps && \
    apt-get clean && \
    mkdir -p /code/PDC_client/PDC_client /data

COPY PDC_client code/PDC_client/PDC_client
COPY setup.py requirements.txt README.md /code/PDC_client

RUN cd /code/PDC_client && \
    python setup.py build && \
    pip install .

RUN echo '#!/usr/bin/env bash\nset -e\nexec "$@"' > /usr/local/bin/entrypoint && \
    chmod 755 /usr/local/bin/entrypoint

# Git version information
ARG GIT_HASH
ARG GIT_SHORT_HASH
ARG GIT_UNCOMMITTED_CHANGES
ARG GIT_LAST_COMMIT
ARG DOCKER_TAG

ENV GIT_HASH=${GIT_HASH}
ENV GIT_SHORT_HASH=${GIT_SHORT_HASH}
ENV GIT_UNCOMMITTED_CHANGES=${GIT_UNCOMMITTED_CHANGES}
ENV GIT_LAST_COMMIT=${GIT_LAST_COMMIT}
ENV DOCKER_TAG=${DOCKER_TAG}

WORKDIR /data
ENTRYPOINT ["/usr/local/bin/entrypoint"]
CMD []

