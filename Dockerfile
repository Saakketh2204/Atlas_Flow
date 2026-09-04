# syntax=docker/dockerfile:1

# ---- Build stage ----
# Uses the official Maven+Temurin image, which resolves dependencies from
# Maven Central at build time -- this requires normal internet access (see
# docs/local-development.md for why that specifically could NOT be done
# inside this project's original sandboxed dev environment, and how the
# core logic was verified there instead).
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /build

# Cache dependency resolution as its own layer, separate from source
# changes, so `docker build` doesn't re-download the world on every
# code edit.
COPY pom.xml .
RUN mvn -B dependency:go-offline

COPY src ./src
RUN mvn -B -DskipTests package

# ---- Runtime stage ----
FROM eclipse-temurin:21-jre-alpine AS runtime
WORKDIR /app

RUN addgroup -S atlasflow && adduser -S atlasflow -G atlasflow
COPY --from=build /build/target/atlasflow.jar app.jar
RUN chown atlasflow:atlasflow app.jar
USER atlasflow

EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
