FROM docker.tech.beegroup-cimne.com/base_dockers/enma-jobs as cached
USER root
RUN apt-get update
RUN python3 -m pip install --upgrade pip
ADD requirements.txt requirements.txt
RUN pip install -r requirements.txt
USER ubuntu
CMD ["python3"]
