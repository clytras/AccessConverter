# AccessConverter container image (02, Packaging): distroless Java 21 and the shaded jar, running as a non-root user.
# Build the jar first (./mvnw -B package), then:
#   docker build -t accessconverter .
#   docker run --rm -u "$(id -u):$(id -g)" -v "$PWD:/data" accessconverter convert Northwind.accdb --to sqlite
# The base is pinned by digest; Dependabot proposes new ones.
FROM gcr.io/distroless/java21-debian12:nonroot@sha256:7e37784d94dccbf5ccb195c73b295f5ad00cd266512dfbac12eb9c3c28f8077d

ARG JAR=target/accessconverter.jar

LABEL org.opencontainers.image.title="AccessConverter" \
      org.opencontainers.image.description="Converts Microsoft Access databases to MySQL/MariaDB dumps, SQLite and JSON" \
      org.opencontainers.image.source="https://github.com/clytras/AccessConverter" \
      org.opencontainers.image.licenses="MIT"

COPY LICENSE NOTICE /opt/accessconverter/
COPY packaging/licenses /opt/accessconverter/licenses/
COPY ${JAR} /opt/accessconverter/accessconverter.jar

# Java takes the encoding of file names and standard output from the locale: under a POSIX one (C), a non-ASCII
# file name can't even be opened, so the image sets a UTF-8 locale
ENV LANG=C.UTF-8
# Relative paths on the command line are relative to the mounted directory
WORKDIR /data
ENTRYPOINT ["java", "-jar", "/opt/accessconverter/accessconverter.jar"]
