package io.lytrax.accessconverter.source;

import static org.assertj.core.api.Assertions.assertThat;

import io.lytrax.accessconverter.Extraction;
import io.lytrax.accessconverter.fixtures.CorpusFile;
import io.lytrax.accessconverter.model.TableModel;
import io.lytrax.accessconverter.profile.DataProfile;
import io.lytrax.accessconverter.profile.DataProfile.ColumnStats;
import io.lytrax.accessconverter.profile.DataProfile.TableProfile;
import io.lytrax.accessconverter.profile.DataProfiler;
import io.lytrax.accessconverter.report.Issue;
import io.lytrax.accessconverter.report.IssueCode;
import io.lytrax.accessconverter.report.Issues;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Which charset Access text is decoded with: the header's code page for Access 97, Unicode after that. */
class TextCharsetTest {
    private static final CorpusFile COMMON2_97 = CorpusFile.get("jackcess/V1997/common2V1997.mdb");
    private static final CorpusFile COMMON2_2000 = CorpusFile.get("jackcess/V2000/common2V2000.mdb");

    @Test
    void anOverrideReplacesTheHeadersCodePageAndIsReported() throws IOException {
        Issues issues = new Issues();
        // 0xA3 is "£" in windows-1252 and "Ј" in windows-1251
        assertThat(currencySymbol(COMMON2_97.file(), new OpenOptions(null, Charset.forName("windows-1251")), issues))
                .isEqualTo("Ј");
        assertThat(issues.list()).singleElement().satisfies(i -> {
            assertThat(i.code()).isEqualTo(IssueCode.CHARSET_OVERRIDDEN);
            assertThat(i.message()).contains("windows-1251").contains("code page 1252");
        });
    }

    @Test
    void unicodeFilesIgnoreAnOverrideAndSaySo() throws IOException {
        Issues issues = new Issues();
        try (AccessSource source = AccessSource.open(
                COMMON2_2000.file(), new OpenOptions(null, Charset.forName("windows-1251")), issues)) {
            assertThat(source.codePage()).isNull();
            assertThat(source.charset()).isEqualTo(StandardCharsets.UTF_16LE);
        }
        assertThat(issues.list()).extracting(Issue::code).containsExactly(IssueCode.CHARSET_IGNORED);
    }

    @Test
    void aCodePageWithoutAJavaCharsetFallsBackToWindows1252AndIsReported(@TempDir Path dir) throws IOException {
        Path file = withCodePage(COMMON2_97.file(), dir.resolve("unmapped.mdb"), 4242);
        Issues issues = new Issues();
        try (AccessSource source = AccessSource.open(file, OpenOptions.DEFAULT, issues)) {
            assertThat(source.codePage()).isEqualTo(4242);
            assertThat(source.charset()).isEqualTo(Charset.forName("windows-1252"));
        }
        assertThat(issues.list()).singleElement().satisfies(i -> {
            assertThat(i.code()).isEqualTo(IssueCode.CHARSET_UNMAPPED);
            assertThat(i.message()).contains("code page 4242").contains("--charset");
        });
    }

    @Test
    void theModelRecordsTheEncoding() {
        assertThat(Extraction.of(COMMON2_97).model().source()).satisfies(s -> {
            assertThat(s.codePage()).isEqualTo(1252);
            assertThat(s.charset()).isEqualTo("windows-1252");
        });
        assertThat(Extraction.of(COMMON2_2000).model().source()).satisfies(s -> {
            assertThat(s.codePage()).isNull();
            assertThat(s.charset()).isEqualTo("UTF-16LE");
        });
    }

    /**
     * indexCodesV1997 holds every byte value; read as code page 1253, three of them (0xAA, 0xD2, 0xFF) are ones
     * Windows decodes to its private use area. They decode that way here too, so nothing is lost; the profile counts
     * the values that hold one, and the conversion reports each such column.
     */
    @Test
    void privateUseTextIsCountedAndReported(@TempDir Path dir) throws IOException {
        CorpusFile indexCodes = CorpusFile.get("jackcess/V1997/indexCodesV1997.mdb");
        Path greek = withCodePage(indexCodes.file(), dir.resolve("greek.mdb"), 1253);
        DataProfile profile = profile(greek);
        List<ColumnStats> privateUse = profile.tables().values().stream()
                .flatMap(t -> t.columns().stream())
                .filter(c -> c.privateUse() != null)
                .toList();
        assertThat(privateUse)
                .isNotEmpty()
                .allSatisfy(c -> assertThat(c.privateUse()).isPositive());
        assertThat(profile.tables().values())
                .flatMap(TableProfile::columns)
                .extracting(ColumnStats::undecodable)
                .containsOnlyNulls();

        Issues issues = new Issues();
        DataProfiler.reportCodePageText(profile, Extraction.of(greek).model().source(), issues);
        assertThat(issues.list()).hasSize(privateUse.size()).allSatisfy(i -> {
            assertThat(i.code()).isEqualTo(IssueCode.TEXT_PRIVATE_USE);
            assertThat(i.message()).contains("code page 1253", "private-use character");
        });

        DataProfile western = profile(indexCodes.file());
        assertThat(western.tables().values()).flatMap(TableProfile::columns).allSatisfy(c -> {
            assertThat(c.undecodable()).isNull();
            assertThat(c.privateUse()).isNull();
        });
    }

    private static DataProfile profile(Path file) throws IOException {
        Extraction extraction = Extraction.of(file, OpenOptions.DEFAULT);
        try (AccessSource source = AccessSource.open(file)) {
            return DataProfiler.profile(source, extraction.model());
        }
    }

    private static String currencySymbol(Path file, OpenOptions options, Issues issues) throws IOException {
        TableModel table = Extraction.of(file, options).table("MSP_PROJECTS");
        try (AccessSource source = AccessSource.open(file, options, issues)) {
            Object[] row = source.rows(table).next();
            return (String) row[
                    table.columns()
                            .indexOf(table.column("PROJ_OPT_CURRENCY_SYMBOL").orElseThrow())];
        }
    }

    /**
     * A copy of a code page 1252 file with another code page in the header. The field (offset 60, little-endian)
     * lies in the part of page 0 that is XOR-ed with a fixed mask, so XOR-ing in old ^ new changes it through the
     * mask.
     */
    static Path withCodePage(Path source, Path target, int codePage) throws IOException {
        byte[] bytes = Files.readAllBytes(source);
        int old = 1252;
        bytes[60] ^= (byte) ((old ^ codePage) & 0xFF);
        bytes[61] ^= (byte) ((old ^ codePage) >> 8);
        return Files.write(target, bytes);
    }
}
