FROM apache/beam_flink1.10_job_server

ARG DOCKER_GID_HOST

RUN groupadd -for -g $DOCKER_GID_HOST docker

RUN apt-get update \
  && curl -fsSL https://get.docker.com -o get-docker.sh \
  && sh get-docker.sh

RUN usermod -aG docker root