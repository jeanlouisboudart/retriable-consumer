FROM maven:3.8-jdk-8 as mavenBuild
COPY pom.xml pom.xml
COPY src src
RUN ["mvn", "install"]

FROM confluentinc/cp-kafka-connect-base:7.4.0
RUN mkdir -p /usr/share/custom-connectors/my-external-service-sink
COPY --from=mavenBuild ./target/*.jar /usr/share/custom-connectors/my-external-service-sink/