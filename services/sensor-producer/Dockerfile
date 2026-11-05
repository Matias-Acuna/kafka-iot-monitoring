FROM maven:3.9.11-eclipse-temurin-21 AS build
WORKDIR /build

COPY pom.xml .
COPY src ./src
RUN mvn -B compile dependency:copy-dependencies \
    -DincludeScope=runtime \
    -DoutputDirectory=/build/target/dependency

FROM eclipse-temurin:21-jre
WORKDIR /app

COPY --from=build /build/target/classes ./classes
COPY --from=build /build/target/dependency ./lib

ENTRYPOINT ["java", "-cp", "/app/classes:/app/lib/*", "com.kafkaiot.producer.SensorProducerApplication"]
