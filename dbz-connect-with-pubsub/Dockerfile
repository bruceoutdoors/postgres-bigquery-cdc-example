FROM debezium/connect:1.1

# COPY cps-kafka-connector.jar $MAVEN_DEP_DESTINATION/cps-kafka-connector.jar

RUN curl -fSL -o cps-kafka-connector.jar \
  https://github.com/bruceoutdoors/pubsub/releases/download/v0.3.1-dbz/cps-kafka-connector.jar \
  && mv cps-kafka-connector.jar $MAVEN_DEP_DESTINATION
