from python:3.9-alpine

MAINTAINER "Aaron Maurais -- MacCoss Lab"

RUN apk add --no-cache git bash && \
    mkdir -p /code/PDC_client /data

RUN git clone https://github.com/ajmaurais/PDC_client /code/PDC_client

RUN cd /code/PDC_client && \
    python setup.py build && \
    pip install .

WORKDIR /data

CMD ["PDC_client"]

