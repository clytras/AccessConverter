package com.lytrax.accessconverter.cli;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.ServiceLoader;
import picocli.CommandLine.Option;

/**
 * Where {@code verify} finds a loaded MySQL or MariaDB database. No JDBC driver ships with the tool (02: the runtime
 * dependencies are Jackcess, sqlite-jdbc, jackson-core and picocli), so the user names one: MariaDB Connector/J
 * talks to both servers, MySQL Connector/J to MySQL.
 */
final class JdbcOptions {

    @Option(
            names = "--jdbc-url",
            paramLabel = "<url>",
            description = "MySQL/MariaDB: the database the dump was imported into, e.g."
                    + " jdbc:mariadb://localhost:3306/northwind or jdbc:mysql://localhost:3306/northwind.")
    String url;

    @Option(
            names = "--jdbc-driver",
            paramLabel = "<jar>",
            description = "The JDBC driver's jar (MariaDB Connector/J or MySQL Connector/J); none is bundled. Not"
                    + " needed when the driver is already on the class path.")
    Path driver;

    @Option(names = "--db-user", paramLabel = "<user>", description = "The database user (or put it in the URL).")
    String user;

    @Option(
            names = "--db-password",
            paramLabel = "<password>",
            defaultValue = "${env:ACCESSCONVERTER_DB_PASSWORD}",
            description = "The database user's password. The ACCESSCONVERTER_DB_PASSWORD environment variable keeps it"
                    + " out of the process list.")
    char[] password;

    boolean given() {
        return url != null;
    }

    Connection connect() throws IOException {
        Properties properties = new Properties();
        if (user != null) {
            properties.setProperty("user", user);
        }
        if (password != null && password.length > 0) {
            properties.setProperty("password", new String(password));
        }
        try {
            if (driver == null) {
                return DriverManager.getConnection(url, properties);
            }
            if (!Files.isRegularFile(driver)) {
                throw new NoSuchFileException(driver.toString(), null, "no such JDBC driver jar");
            }
            // The loader lives as long as the process; the connection needs its classes until then
            @SuppressWarnings("resource")
            URLClassLoader loader =
                    new URLClassLoader(new URL[] {driver.toUri().toURL()}, JdbcOptions.class.getClassLoader());
            for (Driver candidate : ServiceLoader.load(Driver.class, loader)) {
                if (candidate.acceptsURL(url)) {
                    Connection connection = candidate.connect(url, properties);
                    if (connection != null) {
                        return connection;
                    }
                }
            }
            throw new IOException(driver + " has no JDBC driver for " + url);
        } catch (SQLException e) {
            boolean noDriver = driver == null && String.valueOf(e.getMessage()).contains("No suitable driver");
            String hint = noDriver ? "; pass --jdbc-driver with MariaDB Connector/J or MySQL Connector/J" : "";
            throw new IOException("can't connect to " + url + ": " + e.getMessage() + hint, e);
        }
    }
}
