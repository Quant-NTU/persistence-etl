ARG GRADLE_VERSION=8.12.0-jdk21
ARG OPENJDK_VERSION=21-slim

FROM gradle:${GRADLE_VERSION} AS dev
WORKDIR /src

FROM dev AS gradle
COPY --chown=gradle:gradle . /src
RUN gradle clean
RUN gradle build -x test

FROM openjdk:${OPENJDK_VERSION} AS jdk
COPY --from=gradle /src/build/libs/quant-ai-persistence-etl.jar /src/server/server.jar

FROM jdk AS local
EXPOSE 8080
ENTRYPOINT ["java", "-Dspring.profiles.active=local", "-jar", "/src/server/server.jar"]

FROM jdk AS stag
EXPOSE 8080
ENTRYPOINT ["java", "-Dspring.profiles.active=stag", "-jar", "/src/server/server.jar"]

FROM jdk AS prod
EXPOSE 8080
ENTRYPOINT ["java", "-Dspring.profiles.active=prod", "-jar", "/src/server/server.jar"]