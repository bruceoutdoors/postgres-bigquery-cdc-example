# Flink with Docker

https://github.com/knaufk/demo-beam-summit-2018

```sh
cd flink-with-docker/
docker build . -t flink-with-docker --build-arg DOCKER_GID_HOST=$(grep docker /etc/group | cut -d ':' -f 3)
```