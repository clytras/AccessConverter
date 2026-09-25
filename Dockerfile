# syntax=docker/dockerfile:1
# AccessConverter container image: distroless Java 21 and the shaded jar, running as a non-root user.
#
# From a clone, with nothing but Docker installed (the jar is compiled in the first stage; the tests are skipped,
# since they download a test corpus and start database servers):
#   docker build -t accessconverter .
#   docker run --rm -u "$(id -u):$(id -g)" -v "$PWD:/data" accessconverter convert Northwind.accdb --to sqlite
#
# From a jar built beforehand, as the release workflow does with the jar it has tested:
#   docker build --build-arg JAR_SOURCE=prebuilt --build-arg JAR=target/accessconverter-3.0.2.jar -t accessconverter .
#
# The base images are pinned by digest; Dependabot proposes new ones.
ARG JAR_SOURCE=build

# ---- the jar, compiled from the sources
FROM eclipse-temurin:21-jdk@sha256:92a2a4d7a928d057e7bd999c418d66c26a34eb9a0442f3ab67721c3f88110b2d AS build
WORKDIR /src
COPY mvnw pom.xml ./
COPY .mvn .mvn
# The dependencies first, so a change to the sources doesn't download them again
RUN --mount=type=cache,target=/root/.m2 ./mvnw -B -ntp -q dependency:go-offline
COPY src/main src/main
RUN --mount=type=cache,target=/root/.m2 ./mvnw -B -ntp -q package -Dmaven.test.skip=true -Djacoco.skip=true \
    && cp "$(ls target/accessconverter-*.jar | grep -v original-)" /accessconverter.jar

# ---- or the jar built beforehand
FROM scratch AS prebuilt
ARG JAR=target/accessconverter.jar
COPY ${JAR} /accessconverter.jar

FROM ${JAR_SOURCE} AS jar

# ---- the image
FROM gcr.io/distroless/java21-debian12:nonroot@sha256:7e37784d94dccbf5ccb195c73b295f5ad00cd266512dfbac12eb9c3c28f8077d

LABEL org.opencontainers.image.title="AccessConverter" \
      org.opencontainers.image.description="Converts Microsoft Access databases to MySQL/MariaDB dumps, SQLite and JSON" \
      org.opencontainers.image.source="https://github.com/clytras/AccessConverter" \
      org.opencontainers.image.licenses="Apache-2.0"

COPY LICENSE NOTICE /opt/accessconverter/
COPY packaging/licenses /opt/accessconverter/licenses/
COPY --from=jar /accessconverter.jar /opt/accessconverter/accessconverter.jar

# Java takes the encoding of file names and standard output from the locale: under a POSIX one (C), a non-ASCII
# file name can't even be opened, so the image sets a UTF-8 locale
ENV LANG=C.UTF-8
# Relative paths on the command line are relative to the mounted directory
WORKDIR /data
ENTRYPOINT ["java", "-jar", "/opt/accessconverter/accessconverter.jar"]
