# syntax=docker/dockerfile:1
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /src
COPY pom.xml ./
RUN --mount=type=cache,target=/root/.m2 mvn -B -q dependency:go-offline
COPY src src
RUN --mount=type=cache,target=/root/.m2 mvn -B -q -DskipTests package

FROM eclipse-temurin:21-jre
RUN useradd --system --uid 10001 app
USER app
WORKDIR /app
COPY --from=build /src/target/app.jar app.jar
EXPOSE 8080
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75", "-jar", "app.jar"]
