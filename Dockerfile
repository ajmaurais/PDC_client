FROM python:3.11-slim

LABEL maintainer="Aaron Maurais -- MacCoss Lab"

RUN apt-get update && \
    apt-get -y install procps && \
    apt-get clean && \
    mkdir -p /code/PDC_client/PDC_client /data

COPY src code/PDC_client/src
COPY setup.py pyproject.toml requirements.txt README.md /code/PDC_client

RUN cd /code/PDC_client && \
    pip install .

RUN echo '#!/usr/bin/env bash\nset -e\nexec "$@"' > /usr/local/bin/entrypoint && \
    chmod 755 /usr/local/bin/entrypoint

# Git version information
ARG GIT_BRANCH
ARG GIT_REPO
ARG GIT_HASH
ARG GIT_SHORT_HASH
ARG GIT_UNCOMMITTED_CHANGES
ARG GIT_LAST_COMMIT
ARG DOCKER_TAG
ARG DOCKER_IMAGE
ARG PDC_CLIENT_VERSION

ENV GIT_BRANCH=${GIT_BRANCH}
ENV GIT_REPO=${GIT_REPO}
ENV GIT_HASH=${GIT_HASH}
ENV GIT_SHORT_HASH=${GIT_SHORT_HASH}
ENV GIT_UNCOMMITTED_CHANGES=${GIT_UNCOMMITTED_CHANGES}
ENV GIT_LAST_COMMIT=${GIT_LAST_COMMIT}
ENV DOCKER_IMAGE=${DOCKER_IMAGE}
ENV DOCKER_TAG=${DOCKER_TAG}
ARG PDC_CLIENT_VERSION=${PDC_CLIENT_VERSION}

WORKDIR /data
ENTRYPOINT ["/usr/local/bin/entrypoint"]
CMD []

