FROM debezium/connect:1.1

RUN curl -sfSL -o cps-kafka-connector.jar \
  https://github.com/bruceoutdoors/pubsub/releases/download/v0.3-dbz/cps-kafka-connector.jar \
  && mv cps-kafka-connector.jar $MAVEN_DEP_DESTINATION
