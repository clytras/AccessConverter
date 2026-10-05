# syntax=docker/dockerfile:1
# AccessConverter container image: distroless Java 21 and the shaded jar, running as a non-root user.
#
# From a clone, with nothing but Docker installed (the jar is compiled in the first stage; the tests are skipped,
# since they download a test corpus and start database servers):
#   docker build -t accessconverter .
#   docker run --rm -u "$(id -u):$(id -g)" -v "$PWD:/data" accessconverter convert Northwind.accdb --to sqlite
#
# From a jar built beforehand, as the release workflow does with the jar it has tested:
#   docker build --build-arg JAR_SOURCE=prebuilt --build-arg JAR=target/accessconverter-<version>.jar -t accessconverter .
#
# The base images are pinned by digest; Dependabot proposes new ones.
ARG JAR_SOURCE=build

# ---- the jar, compiled from the sources
FROM eclipse-temurin:25-jdk@sha256:8c0a84ea11c8f6ed52600fc19f1040121f2a162998e9f50a5faebbbad9172dcc AS build
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

# ---- sqlite-jdbc's native library for the image's architecture, taken out of the jar here: sqlite-jdbc would unpack
# it into /tmp at run time, which fails with a read-only root filesystem (--read-only) or a noexec /tmp. Unpacking
# doesn't depend on the architecture, so this runs on the build platform, never under emulation.
FROM --platform=$BUILDPLATFORM eclipse-temurin:25-jdk@sha256:8c0a84ea11c8f6ed52600fc19f1040121f2a162998e9f50a5faebbbad9172dcc AS native
ARG TARGETARCH
COPY --from=jar /accessconverter.jar /accessconverter.jar
WORKDIR /unpacked
RUN case "$TARGETARCH" in \
        amd64) arch=x86_64 ;; \
        arm64) arch=aarch64 ;; \
        *) echo "no sqlite-jdbc library is known for $TARGETARCH" >&2; exit 1 ;; \
    esac \
    && jar xf /accessconverter.jar "org/sqlite/native/Linux/$arch/libsqlitejdbc.so" \
    && mkdir /native \
    && mv "org/sqlite/native/Linux/$arch/libsqlitejdbc.so" /native/ \
    && chmod 444 /native/libsqlitejdbc.so

# ---- the image
FROM gcr.io/distroless/java21-debian12:nonroot@sha256:7e37784d94dccbf5ccb195c73b295f5ad00cd266512dfbac12eb9c3c28f8077d

LABEL org.opencontainers.image.title="AccessConverter" \
      org.opencontainers.image.description="Converts Microsoft Access databases to MySQL/MariaDB dumps, SQLite and JSON" \
      org.opencontainers.image.source="https://github.com/clytras/AccessConverter" \
      org.opencontainers.image.licenses="Apache-2.0"

COPY LICENSE NOTICE /opt/accessconverter/
COPY packaging/licenses /opt/accessconverter/licenses/
COPY --from=jar /accessconverter.jar /opt/accessconverter/accessconverter.jar
COPY --from=native /native/libsqlitejdbc.so /opt/accessconverter/native/libsqlitejdbc.so

# Java takes the encoding of file names and standard output from the locale: under a POSIX one (C), a non-ASCII
# file name can't even be opened, so the image sets a UTF-8 locale
ENV LANG=C.UTF-8
# Relative paths on the command line are relative to the mounted directory
WORKDIR /data
# Nothing is written outside the mounted directory, so the container runs with a read-only root filesystem: SQLite's
# library is loaded from where it was put above, and the JVM keeps no performance data in /tmp/hsperfdata_*
ENTRYPOINT ["java", "-XX:-UsePerfData", \
    "-Dorg.sqlite.lib.path=/opt/accessconverter/native", "-Dorg.sqlite.lib.name=libsqlitejdbc.so", \
    "-jar", "/opt/accessconverter/accessconverter.jar"]
