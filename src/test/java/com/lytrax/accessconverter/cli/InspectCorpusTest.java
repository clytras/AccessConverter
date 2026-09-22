package com.lytrax.accessconverter.cli;

import static org.assertj.core.api.Assertions.assertThat;

import com.lytrax.accessconverter.Golden;
import com.lytrax.accessconverter.fixtures.GeneratedFixture;
import com.lytrax.accessconverter.fixtures.JackcessCorpus;
import com.lytrax.accessconverter.fixtures.LocalSample;
import com.lytrax.accessconverter.fixtures.LocalSamples;
import java.nio.file.Path;
import java.util.stream.Stream;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Phase 1 exit criterion: {@code inspect} prints the normalized model for the whole corpus. Tiers A and B are
 * compared with golden files, which also proves the output is identical on every OS, locale and time zone.
 */
class InspectCorpusTest {

    static Stream<Arguments> committedCorpus() {
        return Stream.concat(
                Stream.of(GeneratedFixture.values()).map(f -> Arguments.of(f.fileName(), f.path())),
                Stream.of(JackcessCorpus.values()).map(f -> Arguments.of(f.fileName(), f.path())));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("committedCorpus")
    void matchesTheGoldenModel(String name, Path file) {
        Cli text = Cli.run("inspect", "--profile", file.toString());
        assertThat(text.err()).isEmpty();
        assertThat(text.exitCode()).isIn(ExitCodes.OK, ExitCodes.WARNINGS);
        Golden.assertMatches("inspect/" + name + ".txt", text.out());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("committedCorpus")
    void jsonIsDeterministic(String name, Path file) {
        Cli first = Cli.run("inspect", "--profile", "--format", "json", file.toString());
        Cli second = Cli.run("inspect", "--profile", "--format", "json", file.toString());
        assertThat(first.err()).isEmpty();
        assertThat(first.out()).isEqualTo(second.out()).startsWith("{\n  \"format\": \"accessconverter-inspect\"");
    }

    @Nested
    @LocalSamples
    class Samples {
        @ParameterizedTest
        @EnumSource(LocalSample.class)
        void inspectsEverySample(LocalSample sample) {
            Cli first = Cli.run("inspect", "--profile", sample.path().toString());
            assertThat(first.err()).isEmpty();
            assertThat(first.exitCode()).isIn(ExitCodes.OK, ExitCodes.WARNINGS);
            assertThat(Cli.run("inspect", "--profile", sample.path().toString()).out())
                    .isEqualTo(first.out());
        }
    }
}
