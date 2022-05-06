from python:3.9-alpine

MAINTAINER "Aaron Maurais -- MacCoss Lab"

RUN apk add git openssh && \
    mkdir -p /code/PDC_client /data /root/.ssh

COPY id_rsa_PDC_client_repo /root/.ssh

RUN echo -e 'Host pdcGitRepo\n\tHostname github.com\n\tIdentityFile=/root/.ssh/id_rsa_PDC_client_repo\n\tStrictHostKeyChecking=no' > /root/.ssh/config

RUN git clone git@pdcGitRepo:ajmaurais/PDC_client.git /code/PDC_client
 
RUN cd /code/PDC_client && \
    python setup.py build && \
    pip install .

WORKDIR /data

