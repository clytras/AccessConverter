package com.lytrax.accessconverter.it;

import static org.assertj.core.api.Assertions.assertThat;

import com.lytrax.accessconverter.cli.ExitCodes;
import com.lytrax.accessconverter.cli.Main;
import com.lytrax.accessconverter.fixtures.GeneratedFixture;
import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Statement;
import java.util.stream.Stream;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * The commands as a user runs them against a server: {@code convert --to mysql|mariadb}, the server's client, then
 * {@code verify --jdbc-url}. The test's JDBC drivers are on the class path, so no {@code --jdbc-driver} is needed;
 * an edited value must make verify fail.
 */
class VerifyJdbcIT {

    @TempDir
    static Path dir;

    static Stream<String> images() {
        return DatabaseServer.images().stream();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("images")
    void verifyFindsTheDumpInTheDatabaseAndNoticesAnEdit(String image) throws Exception {
        DatabaseServer server = DatabaseServer.of(image);
        String target = server.dialect().name().toLowerCase(java.util.Locale.ROOT);
        Path dump = dir.resolve(image.replace(':', '-') + "-textEdge.sql");
        String input = GeneratedFixture.TEXT_EDGE.path().toString();

        Run converted = run("convert", input, "--to", target, "-o", dump.toString(), "--no-report");
        assertThat(converted.code()).as(converted.err()).isEqualTo(ExitCodes.OK);
        server.recreate("cli");
        assertThat(server.importDump(dump, "cli")).isEmpty();

        String[] verify = {
            "verify",
            input,
            "--jdbc-url",
            server.jdbcUrl("cli"),
            "--db-user",
            "root",
            "--db-password",
            server.rootPassword()
        };
        Run matched = run(verify);
        assertThat(matched.code()).as(matched.out() + matched.err()).isEqualTo(ExitCodes.OK);
        assertThat(matched.out()).contains("verify: the output matches the source");

        try (Connection db = server.connect("cli");
                Statement s = db.createStatement()) {
            s.execute("UPDATE TextEdge SET Val = CONCAT(Val, 'x') WHERE ID = 1");
        }
        Run edited = run(verify);
        assertThat(edited.code()).isEqualTo(ExitCodes.FAILED);
        assertThat(edited.out()).contains("1 differences");
    }

    private record Run(int code, String out, String err) {}

    private static Run run(String... args) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        StringWriter err = new StringWriter();
        int code = Main.run(out, StandardCharsets.UTF_8, new PrintWriter(err, true), args);
        return new Run(code, out.toString(StandardCharsets.UTF_8), err.toString());
    }
}
