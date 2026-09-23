package com.lytrax.accessconverter.cli;

import static org.assertj.core.api.Assertions.assertThat;

import com.lytrax.accessconverter.Golden;
import com.lytrax.accessconverter.fixtures.CorpusCase;
import com.lytrax.accessconverter.fixtures.CorpusFile;
import com.lytrax.accessconverter.fixtures.LocalSample;
import com.lytrax.accessconverter.fixtures.LocalSamples;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Phase 1 exit criterion, widened to every database Jackcess can read: {@code inspect} prints the normalized model
 * for the whole corpus. Tiers A and B are compared with golden files, which also proves the output is identical on
 * every OS, locale and time zone. Tier B's non-databases must fail with one clear error line.
 */
class InspectCorpusTest {

    static Stream<CorpusCase> databases() {
        return CorpusCase.databases();
    }

    static Stream<CorpusFile> notDatabases() {
        return CorpusFile.all().stream().filter(f -> !f.isDatabase());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("databases")
    void matchesTheGoldenModel(CorpusCase database) {
        Cli text = Cli.run(args(database, "inspect", "--profile"));
        assertThat(text.err()).isEmpty();
        assertThat(text.exitCode()).isIn(ExitCodes.OK, ExitCodes.WARNINGS);
        Golden.assertMatches("inspect/" + database.id() + ".txt", text.out());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("databases")
    void jsonIsDeterministic(CorpusCase database) {
        Cli first = Cli.run(args(database, "inspect", "--profile", "--format", "json"));
        Cli second = Cli.run(args(database, "inspect", "--profile", "--format", "json"));
        assertThat(first.err()).isEmpty();
        assertThat(first.out()).isEqualTo(second.out()).startsWith("{\n  \"format\": \"accessconverter-inspect\"");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("notDatabases")
    void aFileThatIsNotADatabaseFailsWithOneLine(CorpusFile file) {
        Cli cli = Cli.run("inspect", file.file().toString());
        assertThat(cli.exitCode()).isEqualTo(ExitCodes.FAILED);
        assertThat(cli.out()).isEmpty();
        assertThat(cli.err().lines())
                .containsExactly("error: " + file.fileName()
                        + ": not an Access database (no Jet or ACE header); Access 97 through Microsoft 365 files are"
                        + " supported");
    }

    private static String[] args(CorpusCase database, String... command) {
        List<String> args = new ArrayList<>(List.of(command));
        if (database.options().password() != null) {
            args.add("--password");
            args.add(database.options().password());
        }
        args.add(database.file().toString());
        return args.toArray(String[]::new);
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
