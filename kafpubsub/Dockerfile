# Same image as Debezium
FROM fabric8/java-centos-openjdk11-jdk

WORKDIR /kafpubsub/

COPY target/kafpubsub.jar /kafpubsub/

ENTRYPOINT ["java", "-jar", "kafpubsub.jar"]
