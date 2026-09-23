package com.lytrax.accessconverter.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.utility.DockerImageName;

/**
 * The 09 server matrix's own check: each image in {@code -Dit.db.images} (all five by default, see pom.xml) starts,
 * imports a dump through its own client with no flags, reports a broken one as an error, and is read back over JDBC.
 * The converter's dumps are {@code MySqlCorpusIT}'s.
 */
class ServerMatrixIT {
    private static final String TEXT = "Γιώργος 😀";

    private static final String DUMP = """
            SET NAMES utf8mb4;
            SET SESSION sql_mode = 'STRICT_ALL_TABLES,NO_ENGINE_SUBSTITUTION';
            CREATE TABLE smoke (id INT NOT NULL PRIMARY KEY, label VARCHAR(50) NOT NULL) DEFAULT CHARSET = utf8mb4;
            INSERT INTO smoke VALUES (1, '%s');
            """.formatted(TEXT);

    static Stream<String> images() {
        return DatabaseServer.images().stream();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("images")
    void importsWithTheServerClientAndReadsBackOverJdbc(String image, @TempDir Path tmp) throws Exception {
        Path good = Files.writeString(tmp.resolve("good.sql"), DUMP);
        Path broken = Files.writeString(tmp.resolve("broken.sql"), "CREATE TABLE broken (;\n");

        DatabaseServer server = DatabaseServer.of(image);
        server.recreate("smoke");
        server.importDump(good, "smoke");
        assertThatThrownBy(() -> server.importDump(broken, "smoke"))
                .isInstanceOf(DatabaseServer.DumpImportException.class)
                .hasMessageContaining("ERROR 1064");
        {
            try (Connection c = server.connect("smoke");
                    Statement s = c.createStatement();
                    ResultSet rs = s.executeQuery("SELECT VERSION(), label FROM smoke WHERE id = 1")) {
                assertThat(rs.next()).isTrue();
                String tag = DockerImageName.parse(image).getVersionPart();
                assertThat(rs.getString(1)).matches(Pattern.quote(tag) + "([.-].*)?");
                assertThat(rs.getString(2)).isEqualTo(TEXT);
            }
        }
    }
}
