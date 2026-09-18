FROM flink:1.19.2-scala_2.12-java17

ARG KAFKA_CONNECTOR=3.2.0-1.19
RUN curl -fSL -o /opt/flink/lib/flink-sql-connector-kafka-${KAFKA_CONNECTOR}.jar \
    https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/${KAFKA_CONNECTOR}/flink-sql-connector-kafka-${KAFKA_CONNECTOR}.jar
