package com.lytrax.accessconverter.it;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.mariadb.MariaDBContainer;
import org.testcontainers.mysql.MySQLContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

/**
 * A MySQL or MariaDB server in Docker. Dumps are imported with the server's own command-line client, the way users
 * import them (09), and read back over JDBC.
 */
final class DatabaseServer implements AutoCloseable {
    private static final String DUMP_IN_CONTAINER = "/tmp/import.sql";

    private final JdbcDatabaseContainer<?> container;
    private final String client;

    private DatabaseServer(JdbcDatabaseContainer<?> container, String client) {
        this.container = container;
        this.client = client;
    }

    /** Starts {@code mysql:*} or {@code mariadb:*}; the image name decides the dialect. */
    static DatabaseServer start(String image) {
        DockerImageName name = DockerImageName.parse(image);
        DatabaseServer server =
                switch (name.getRepository()) {
                    case "mysql" -> new DatabaseServer(new MySQLContainer(name), "mysql");
                    // MariaDB 11 images no longer ship the "mysql" client alias
                    case "mariadb" -> new DatabaseServer(new MariaDBContainer(name), "mariadb");
                    default -> throw new IllegalArgumentException("not a MySQL or MariaDB image: " + image);
                };
        server.container.start();
        return server;
    }

    /** Pipes a dump through the server's client. A non-zero exit fails with the client's error output. */
    void importDump(Path dump) throws IOException, InterruptedException {
        container.copyFileToContainer(MountableFile.forHostPath(dump), DUMP_IN_CONTAINER);
        String command = String.join(
                " ",
                client,
                "--default-character-set=utf8mb4",
                "--user=" + container.getUsername(),
                "--password=" + container.getPassword(),
                container.getDatabaseName(),
                "<",
                DUMP_IN_CONTAINER);
        ExecResult result = container.execInContainer("sh", "-c", command);
        if (result.getExitCode() != 0) {
            throw new DumpImportException(result.getExitCode(), result.getStderr());
        }
    }

    Connection connect() throws SQLException {
        return container.createConnection("");
    }

    @Override
    public void close() {
        container.stop();
    }

    static final class DumpImportException extends IOException {
        private static final long serialVersionUID = 1L;

        DumpImportException(int exitCode, String stderr) {
            super("dump import failed with exit code " + exitCode + ": " + stderr.strip());
        }
    }
}
