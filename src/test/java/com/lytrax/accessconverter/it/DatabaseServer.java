package com.lytrax.accessconverter.it;

import com.lytrax.accessconverter.target.mysql.MySqlDialect;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.mariadb.MariaDBContainer;
import org.testcontainers.mysql.MySQLContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

/**
 * A MySQL or MariaDB server in Docker. Dumps are imported with the server's own command-line client, the way users
 * import them (09), and read back over JDBC. Each server is started once per test run and shared by the tests.
 */
final class DatabaseServer {
    private static final String DUMP_IN_CONTAINER = "/tmp/import.sql";

    private static final Map<String, DatabaseServer> STARTED = new ConcurrentHashMap<>();

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> STARTED.values().forEach(s -> s.container.stop())));
    }

    private final String image;
    private final JdbcDatabaseContainer<?> container;
    private final String client;
    private final MySqlDialect dialect;

    private DatabaseServer(String image, JdbcDatabaseContainer<?> container, String client, MySqlDialect dialect) {
        this.image = image;
        this.container = container;
        this.client = client;
        this.dialect = dialect;
    }

    /** Each image in {@code -Dit.db.images} (all five by default, see pom.xml). */
    static List<String> images() {
        return Arrays.stream(System.getProperty("it.db.images", "").split(","))
                .map(String::strip)
                .filter(image -> !image.isEmpty())
                .toList();
    }

    /** The running server for {@code mysql:*} or {@code mariadb:*}; the image name decides the dialect. */
    static synchronized DatabaseServer of(String image) {
        return STARTED.computeIfAbsent(image, DatabaseServer::start);
    }

    private static DatabaseServer start(String image) {
        DockerImageName name = DockerImageName.parse(image);
        DatabaseServer server =
                switch (name.getRepository()) {
                    case "mysql" -> new DatabaseServer(image, new MySQLContainer(name), "mysql", MySqlDialect.MYSQL);
                    // MariaDB 11 images no longer ship the "mysql" client alias
                    case "mariadb" ->
                        new DatabaseServer(image, new MariaDBContainer(name), "mariadb", MySqlDialect.MARIADB);
                    default -> throw new IllegalArgumentException("not a MySQL or MariaDB image: " + image);
                };
        server.container.start();
        return server;
    }

    String image() {
        return image;
    }

    MySqlDialect dialect() {
        return dialect;
    }

    /** A new, empty database. */
    void recreate(String database) throws SQLException {
        try (Connection c = connect("");
                Statement s = c.createStatement()) {
            s.execute("DROP DATABASE IF EXISTS `" + database + "`");
            s.execute("CREATE DATABASE `" + database + "`");
        }
    }

    /**
     * Pipes a dump through the server's own client, with nothing but the credentials and the database: no character
     * set, no mode, no other flag. {@code --show-warnings} only makes the client print what the server warned about,
     * which is the output this returns; a failed import throws with the client's error output.
     */
    String importDump(Path dump, String database) throws IOException, InterruptedException {
        container.copyFileToContainer(MountableFile.forHostPath(dump), DUMP_IN_CONTAINER);
        String command = String.join(
                " ",
                client,
                "--user=root",
                "--password=" + container.getPassword(),
                "--show-warnings",
                database,
                "<",
                DUMP_IN_CONTAINER);
        ExecResult result = container.execInContainer("sh", "-c", command);
        if (result.getExitCode() != 0) {
            throw new DumpImportException(result.getExitCode(), result.getStderr() + result.getStdout());
        }
        return (result.getStdout() + stderrWithoutPasswordNotice(result.getStderr())).strip();
    }

    private static String stderrWithoutPasswordNotice(String stderr) {
        return stderr.lines()
                .filter(line -> !line.contains("Using a password on the command line"))
                .collect(Collectors.joining("\n"));
    }

    /** A root connection to a database; an empty name for none. */
    Connection connect(String database) throws SQLException {
        String url = container
                .getJdbcUrl()
                .replaceFirst("/" + container.getDatabaseName() + "(\\?|$)", "/" + database + "$1");
        return DriverManager.getConnection(url, "root", container.getPassword());
    }

    /** The JDBC URL of a database, as {@code verify --jdbc-url} takes it. */
    String jdbcUrl(String database) {
        return container
                .getJdbcUrl()
                .replaceFirst("/" + container.getDatabaseName() + "(\\?|$)", "/" + database + "$1");
    }

    /** {@code host:port} of the server, for a URL of another driver than the container's own. */
    String address() {
        return container.getHost() + ":" + container.getMappedPort(3306);
    }

    String rootPassword() {
        return container.getPassword();
    }

    static final class DumpImportException extends IOException {
        private static final long serialVersionUID = 1L;

        DumpImportException(int exitCode, String output) {
            super("dump import failed with exit code " + exitCode + ": " + output.strip());
        }
    }
}
