package com.lytrax.accessconverter.it;

import static org.assertj.core.api.Assertions.assertThat;

import com.lytrax.accessconverter.fixtures.LocalSample;
import com.lytrax.accessconverter.fixtures.LocalSamples;
import com.lytrax.accessconverter.report.Severity;
import com.lytrax.accessconverter.source.OpenOptions;
import com.lytrax.accessconverter.target.BinaryMode;
import com.lytrax.accessconverter.target.ConvertOptions;
import com.lytrax.accessconverter.target.mysql.MySqlFixture;
import com.lytrax.accessconverter.target.mysql.MySqlFixture.Converted;
import com.lytrax.accessconverter.target.mysql.MySqlOptions;
import com.lytrax.accessconverter.verify.VerifyResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.time.Duration;
import java.util.stream.Stream;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tier C on every server (gitignored samples, so only on the maintainer's machine; skipped elsewhere): the databases
 * v2's MySQL dumps broke import cleanly and verify, the Greek Access 97 Northwind keeps its foreign keys, and
 * MarketBasket's dump is written inside 03's budget.
 */
@LocalSamples
class MySqlLocalSamplesIT {

    @TempDir
    static Path dir;

    static Stream<Arguments> cases() {
        return DatabaseServer.images().stream()
                .flatMap(image -> Stream.of(LocalSample.values()).map(sample -> Arguments.of(image, sample)));
    }

    @ParameterizedTest(name = "{0} {1}")
    @MethodSource("cases")
    void importsCleanlyAndVerifies(String image, LocalSample sample) throws Exception {
        Path source = sample.path();
        DatabaseServer server = DatabaseServer.of(image);
        Path dump = dir.resolve(image.replace(':', '-') + "-" + sample.name() + ".sql");
        long started = System.nanoTime();
        Converted converted = MySqlFixture.convert(source, dump, server.dialect());
        Duration took = Duration.ofNanos(System.nanoTime() - started);
        assertThat(converted.issues().list()).noneMatch(issue -> issue.severity() == Severity.ERROR);
        if (sample == LocalSample.MARKET_BASKET) {
            // 262,704 rows; 03's budget is ten seconds in every target
            assertThat(took).isLessThan(Duration.ofSeconds(10));
        }
        if (sample == LocalSample.NORTHWIND_97) {
            // Jackcess's index seek misses rows in this file; trusting it would have cost every foreign key (04)
            assertThat(converted.table("Orders").foreignKeys()).hasSize(3);
            assertThat(converted.table("Order Details").foreignKeys()).hasSize(2);
        }

        String db = "s_" + sample.name().toLowerCase(java.util.Locale.ROOT);
        server.recreate(db);
        assertThat(server.importDump(dump, db)).isEmpty();
        try (Connection connection = server.connect(db)) {
            VerifyResult verified = converted.verify(connection);
            assertThat(verified.differences()).isEmpty();
        }

        // 08's options: OLE values decoded, the bytes in files, version history as child tables
        Path files = Files.createDirectories(dir.resolve(image.replace(':', '-') + "-files"));
        Path extended = files.resolve(sample.name() + ".sql");
        Converted withFiles = MySqlFixture.convert(
                source,
                extended,
                OpenOptions.DEFAULT,
                ConvertOptions.DEFAULT.withBinary(BinaryMode.FILES, true, true),
                MySqlOptions.of(server.dialect()));
        assertThat(withFiles.issues().list()).noneMatch(issue -> issue.severity() == Severity.ERROR);
        server.recreate(db);
        assertThat(server.importDump(extended, db)).isEmpty();
        try (Connection connection = server.connect(db)) {
            assertThat(withFiles.verify(connection, files).differences()).isEmpty();
        }
    }
}
