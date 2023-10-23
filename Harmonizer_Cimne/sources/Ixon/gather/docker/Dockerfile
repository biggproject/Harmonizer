FROM docker.tech.beegroup-cimne.com/admin/mr-docker:latest
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update
RUN apt-get install -y python3 python3-pip openvpn
RUN apt-get install -y sudo

ADD hadoop_stack /hadoop_stack
ADD requirements.txt requirements.txt

RUN pip install -r requirements.txt

RUN usermod -aG sudo ubuntu

RUN echo ubuntu:ubuntu | chpasswd

RUN echo "ubuntu ALL=(ALL:ALL) NOPASSWD: ALL" > /etc/sudoers.d/ubuntu

USER ubuntu

CMD ["python3"]
