from python:3.9-alpine

MAINTAINER "Aaron Maurais -- MacCoss Lab"

RUN apk add --no-cache git bash && \
    mkdir -p /code/PDC_client/PDC_client /data

COPY PDC_client code/PDC_client/PDC_client
COPY setup.py requirements.txt README.md /code/PDC_client

RUN cd /code/PDC_client && \
    python setup.py build && \
    pip install .

WORKDIR /data

CMD ["PDC_client"]

