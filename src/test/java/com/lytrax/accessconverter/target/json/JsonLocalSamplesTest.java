package com.lytrax.accessconverter.target.json;

import static org.assertj.core.api.Assertions.assertThat;

import com.lytrax.accessconverter.fixtures.LocalSample;
import com.lytrax.accessconverter.fixtures.LocalSamples;
import com.lytrax.accessconverter.report.Severity;
import com.lytrax.accessconverter.source.OpenOptions;
import com.lytrax.accessconverter.target.BinaryMode;
import com.lytrax.accessconverter.target.ConvertOptions;
import com.lytrax.accessconverter.target.json.JsonFixture.Converted;
import com.lytrax.accessconverter.target.json.JsonOptions.Layout;
import java.nio.file.Path;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Tier C: the maintainer's real databases (gitignored, so only on their machine, with {@code -Plocal-samples}),
 * among them {@code allTypes_mdb}, whose empty OLE value crashed v2's JSON export (F-13b).
 */
@LocalSamples
class JsonLocalSamplesTest {

    @TempDir
    static Path dir;

    @ParameterizedTest
    @EnumSource(LocalSample.class)
    void exportsValidatesAndVerifiesEverySample(LocalSample sample) {
        for (Layout layout : Layout.values()) {
            Path output = dir.resolve(sample.name() + (layout == Layout.DOCUMENT ? ".json" : "-ndjson"));
            Converted converted = JsonFixture.convert(sample.path(), output, JsonOptions.DEFAULT.withLayout(layout));

            assertThat(converted.issues().list()).noneMatch(issue -> issue.severity() == Severity.ERROR);
            assertThat(JsonSchemaCheck.errors(output)).isEmpty();
            assertThat(converted.verify().differences()).isEmpty();
        }
        // 08's options: OLE objects, files, version history
        Path output = dir.resolve(sample.name() + "-files.json");
        Converted converted = JsonFixture.convert(
                sample.path(),
                output,
                OpenOptions.DEFAULT,
                ConvertOptions.DEFAULT.withBinary(BinaryMode.FILES, true, true),
                JsonOptions.DEFAULT);
        assertThat(converted.issues().list()).noneMatch(issue -> issue.severity() == Severity.ERROR);
        assertThat(JsonSchemaCheck.errors(output)).isEmpty();
        assertThat(converted.verify().differences()).isEmpty();
    }

    @Test
    void theGreekAccess97NorthwindKeepsEveryRelationship() {
        Converted converted = JsonFixture.convert(LocalSample.NORTHWIND_97.path(), dir.resolve("nw97.json"));
        // Jackcess 5.0.1 can't use this file's catalog index; before the fallback it gave 3 of 8 tables and none of
        // the 7 relationships
        assertThat(converted.plan().tables()).hasSize(8);
        assertThat(converted.plan().relationships()).hasSize(7);
    }

    @Test
    void marketBasketExportsInsideTheBudget() {
        long started = System.nanoTime();
        Converted converted = JsonFixture.convert(LocalSample.MARKET_BASKET.path(), dir.resolve("marketBasket.json"));
        Duration took = Duration.ofNanos(System.nanoTime() - started);

        assertThat(converted.outcome().rowsWritten()).isEqualTo(262_704);
        assertThat(took).as("%s", took).isLessThan(Duration.ofSeconds(10));
    }
}
