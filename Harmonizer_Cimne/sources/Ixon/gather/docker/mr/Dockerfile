FROM docker.tech.beegroup-cimne.com/admin/enma-job:latest

USER root
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y build-essential libsasl2-dev
WORKDIR /home/ubuntu/ixon_mr
RUN chown -R ubuntu:root /home/ubuntu/

USER ubuntu

RUN mkdir "vpn_files"

COPY ../requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY ixon_mrjob.py ixon_mrjob.py
COPY ixon_gather.py ixon_gather.py
COPY Ixon.py Ixon.py
COPY utils.py utils.py