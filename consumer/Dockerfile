FROM maven:3.8-jdk-8 as mavenBuild
COPY pom.xml pom.xml
COPY src src
RUN ["mvn", "install"]

FROM java:8
COPY --from=mavenBuild ./target/*.jar ./
ENV JAVA_OPTS ""
ENV MAIN_CLASS "com.sample.NoRetryConsumer"
CMD [ "bash", "-c", "java ${JAVA_OPTS} -cp *.jar ${MAIN_CLASS}"]