FROM maven:3.8-openjdk-11 AS builder
WORKDIR /app
COPY pom.xml .
COPY src ./src
RUN mvn clean package -DskipTests

FROM flink:1.17.1-scala_2.12
COPY --from=builder /app/target/flink-event-log-processor-1.0-SNAPSHOT.jar /opt/flink/usrlib/job.jar
# Wait for JobManager and submit
CMD ["/bin/bash", "-c", "while ! (echo > /dev/tcp/flink/8081) >/dev/null 2>&1; do sleep 5; echo 'Waiting for JobManager...'; done; flink run -m flink:8081 /opt/flink/usrlib/job.jar"]
