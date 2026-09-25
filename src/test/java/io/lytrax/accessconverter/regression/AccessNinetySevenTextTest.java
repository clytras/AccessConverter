package io.lytrax.accessconverter.regression;

import static org.assertj.core.api.Assertions.assertThat;

import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.DatabaseBuilder;
import com.healthmarketscience.jackcess.Row;
import com.healthmarketscience.jackcess.crypt.CryptCodecProvider;
import io.lytrax.accessconverter.Extraction;
import io.lytrax.accessconverter.Golden;
import io.lytrax.accessconverter.fixtures.CorpusFile;
import io.lytrax.accessconverter.model.ColumnModel;
import io.lytrax.accessconverter.model.TableModel;
import io.lytrax.accessconverter.report.Issues;
import io.lytrax.accessconverter.source.AccessSource;
import io.lytrax.accessconverter.source.RowStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Access 97 (Jet 3) stores text in its header's code page. Jackcess decodes it with Java's default charset, UTF-8
 * since Java 18, which turned "£" into U+FFFD. The source layer reopens Jet 3 files with the code page's charset.
 */
class AccessNinetySevenTextTest {
    private static final Charset WINDOWS_1252 = Charset.forName("windows-1252");
    /** U+FFFD, what an undecodable byte turns into. */
    private static final int REPLACEMENT = 0xFFFD;

    private static final Set<Integer> UNDEFINED_IN_1252 = Set.of(0x81, 0x8D, 0x8F, 0x90, 0x9D);

    static Stream<CorpusFile> access97() {
        return CorpusFile.databases().stream().filter(f -> {
            try (Database db = f.open()) {
                return db.getFileFormat() == Database.FileFormat.V1997;
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        });
    }

    @Test
    void thePoundSignSurvives() throws IOException {
        CorpusFile common2 = CorpusFile.get("jackcess/V1997/common2V1997.mdb");
        TableModel table = Extraction.of(common2).table("MSP_PROJECTS");
        try (AccessSource source = AccessSource.open(common2.file(), common2.openOptions(), new Issues())) {
            assertThat(source.codePage()).isEqualTo(1252);
            assertThat(source.charset()).isEqualTo(WINDOWS_1252);
            Object[] row = source.rows(table).next();
            int symbol = table.columns()
                    .indexOf(table.column("PROJ_OPT_CURRENCY_SYMBOL").orElseThrow());
            assertThat(row[symbol]).isEqualTo("£");
        }
    }

    /**
     * Every text value equals the Windows code page 1252 decoding of its stored bytes, and none is lost to U+FFFD
     * (indexCodesV1997 holds every byte value). The bytes come from a second open with ISO-8859-1, which maps each
     * byte to the char of the same value, so nothing is lost on the way.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("access97")
    void everyValueIsTheWindows1252DecodingOfItsBytes(CorpusFile file) throws IOException {
        Extraction extraction = Extraction.of(file);
        try (AccessSource source = AccessSource.open(file.file(), file.openOptions(), new Issues());
                Database bytes = new DatabaseBuilder(file.file())
                        .setReadOnly(true)
                        .setCodecProvider(new CryptCodecProvider(file.password()))
                        .setCharset(StandardCharsets.ISO_8859_1)
                        .open()) {
            for (TableModel table : extraction.model().tables()) {
                List<ColumnModel> text = textColumns(table);
                if (text.isEmpty()) {
                    continue;
                }
                RowStream decoded = source.scan(table, text);
                Iterator<Row> raw = bytes.getTable(table.name()).iterator();
                while (decoded.hasNext()) {
                    Object[] values = decoded.next();
                    Row row = raw.next();
                    for (int i = 0; i < values.length; i++) {
                        Object stored = row.get(text.get(i).name());
                        String expected = stored == null
                                ? null
                                : windows1252(((String) stored).getBytes(StandardCharsets.ISO_8859_1));
                        assertThat(values[i])
                                .as("%s.%s", table.name(), text.get(i).name())
                                .isEqualTo(expected);
                        assertThat(values[i] == null || ((String) values[i]).indexOf(REPLACEMENT) < 0)
                                .as("%s.%s decodes", table.name(), text.get(i).name())
                                .isTrue();
                    }
                }
                assertThat(raw.hasNext()).isFalse();
            }
        }
    }

    /** The decoded text itself, pinned: a change in decoding shows up as a readable diff. */
    @ParameterizedTest
    @ValueSource(strings = {"jackcess/V1997/common2V1997.mdb", "jackcess/V1997/indexCodesV1997.mdb"})
    void decodedTextMatchesItsGolden(String id) throws IOException {
        CorpusFile file = CorpusFile.get(id);
        Extraction extraction = Extraction.of(file);
        StringBuilder out = new StringBuilder();
        try (AccessSource source = AccessSource.open(file.file(), file.openOptions(), new Issues())) {
            for (TableModel table : extraction.model().tables()) {
                List<ColumnModel> text = textColumns(table);
                if (text.isEmpty()) {
                    continue;
                }
                out.append("# ").append(table.name()).append('\n');
                RowStream rows = source.scan(table, text);
                for (int row = 1; rows.hasNext(); row++) {
                    Object[] values = rows.next();
                    for (int i = 0; i < values.length; i++) {
                        if (values[i] != null) {
                            out.append(row)
                                    .append('\t')
                                    .append(text.get(i).name())
                                    .append('\t')
                                    .append(escape((String) values[i]))
                                    .append('\n');
                        }
                    }
                }
            }
        }
        Golden.assertMatches("text/" + id + ".txt", out.toString());
    }

    /**
     * Code page 1252 as Windows decodes it, independently of the converter: the JDK's windows-1252, except for the
     * five bytes it leaves undefined, which Windows (and Access) decode to the C1 control of the same value.
     */
    private static String windows1252(byte[] bytes) {
        StringBuilder out = new StringBuilder();
        for (byte b : bytes) {
            int value = b & 0xFF;
            out.append(
                    UNDEFINED_IN_1252.contains(value)
                            ? String.valueOf((char) value)
                            : new String(new byte[] {b}, WINDOWS_1252));
        }
        return out.toString();
    }

    private static List<ColumnModel> textColumns(TableModel table) {
        return table.columns().stream().filter(c -> c.type().isText()).toList();
    }

    /** Printable text as is; control characters, backslashes and unassigned ones as escapes. */
    static String escape(String s) {
        StringBuilder out = new StringBuilder();
        s.codePoints().forEach(cp -> {
            if (cp == '\\') {
                out.append("\\\\");
            } else if (cp < 0x20 || (cp >= 0x7F && cp < 0xA0) || !Character.isDefined(cp)) {
                out.append(String.format(Locale.ROOT, "\\u%04X", cp));
            } else {
                out.appendCodePoint(cp);
            }
        });
        return out.toString();
    }
}
