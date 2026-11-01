FROM gradle:8.14.3-jdk21 AS build
WORKDIR /workspace
COPY settings.gradle.kts build.gradle.kts ./
COPY src ./src
RUN gradle --no-daemon installDist
FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /workspace/build/install/taskflow/ ./
EXPOSE 8080
USER 10001
ENTRYPOINT ["/app/bin/taskflow"]
