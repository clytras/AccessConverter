package com.lytrax.accessconverter.it;

import static org.assertj.core.api.Assertions.assertThat;

import com.lytrax.accessconverter.fixtures.CorpusCase;
import com.lytrax.accessconverter.report.Severity;
import com.lytrax.accessconverter.target.ConvertOptions;
import com.lytrax.accessconverter.target.mysql.MySqlFixture;
import com.lytrax.accessconverter.target.mysql.MySqlFixture.Converted;
import com.lytrax.accessconverter.target.mysql.MySqlOptions;
import com.lytrax.accessconverter.verify.VerifyResult;
import java.nio.file.Path;
import java.sql.Connection;
import java.util.stream.Stream;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Phase 3's exit criterion (05, Acceptance criteria): every database of tiers A, B and D converts for the server's
 * dialect, imports through the server's own client with no flags and with no error and no warning (notes
 * included), and {@code verify} then finds the schema and every value where the plan says they should be.
 */
class MySqlCorpusIT {

    @TempDir
    static Path dir;

    static Stream<Arguments> cases() {
        return DatabaseServer.images().stream()
                .flatMap(image -> CorpusCase.databases().map(database -> Arguments.of(image, database)));
    }

    @ParameterizedTest(name = "{0} {1}")
    @MethodSource("cases")
    void importsCleanlyAndVerifies(String image, CorpusCase database) throws Exception {
        DatabaseServer server = DatabaseServer.of(image);
        String name = database.id().replaceAll("[^A-Za-z0-9]", "_");
        Path dump = dir.resolve(image.replace(':', '-') + "_" + name + ".sql");
        Converted converted = MySqlFixture.convert(
                database.file(), dump, database.options(), ConvertOptions.DEFAULT, MySqlOptions.of(server.dialect()));
        assertThat(converted.issues().list())
                .as("errors in the conversion report")
                .noneMatch(issue -> issue.severity() == Severity.ERROR);

        String db = "c_" + Integer.toHexString(name.hashCode());
        server.recreate(db);
        assertThat(server.importDump(dump, db))
                .as("what the client printed (warnings)")
                .isEmpty();

        try (Connection connection = server.connect(db)) {
            VerifyResult verified = converted.verify(connection);
            assertThat(verified.differences()).isEmpty();
            assertThat(verified.matches()).isTrue();
        }
    }
}
