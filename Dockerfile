from python:3.9-slim

MAINTAINER "Aaron Maurais -- MacCoss Lab"

RUN mkdir -p /code/PDC_client/PDC_client /data

COPY PDC_client code/PDC_client/PDC_client
COPY setup.py requirements.txt README.md /code/PDC_client

RUN cd /code/PDC_client && \
    python setup.py build && \
    pip install .

RUN echo '#!/usr/bin/env bash\nset -e\nexec "$@"' > /usr/local/bin/entrypoint && \
    chmod 755 /usr/local/bin/entrypoint

WORKDIR /data
ENTRYPOINT ["/usr/local/bin/entrypoint"]
CMD []

