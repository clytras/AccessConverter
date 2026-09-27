package io.lytrax.accessconverter.target.mysql;

import static org.assertj.core.api.Assertions.assertThat;

import com.healthmarketscience.jackcess.ColumnBuilder;
import com.healthmarketscience.jackcess.DataType;
import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.DatabaseBuilder;
import com.healthmarketscience.jackcess.Table;
import com.healthmarketscience.jackcess.TableBuilder;
import io.lytrax.accessconverter.cli.Main;
import io.lytrax.accessconverter.fixtures.CorpusFile;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Memory doesn't grow with the largest value (03, Streaming): a dump is written in the heap a JSON export of the same
 * database needs, and is byte for byte the dump an unlimited heap writes. A row larger than {@code --batch-bytes} goes
 * out on its own, its binary values hexed a chunk at a time; it used to be built whole, several times its size.
 *
 * <p>Memory is what this measures, so the conversions under a limit run in a JVM of their own.
 */
class MySqlLargeValueMemoryTest {

    @TempDir
    Path dir;

    /** mdb-reader's OLE file: 11 values, the largest 5.9 MB. It needed between 64 and 96 MB. */
    @Test
    void aDatabaseOfOleValuesDumpsInTheHeapJsonNeeds() throws Exception {
        Path source = CorpusFile.get("mdb-reader/test/data/V2007/ole.accdb").file();
        dumpsWithin(source, "48m");
    }

    /** One 50 MB Binary value, which needed 384 MB. */
    @Test
    void aFiftyMegabyteValueDumpsInTheHeapJsonNeeds() throws Exception {
        Path source = dir.resolve("large-value.accdb");
        try (Database db = new DatabaseBuilder(source.toFile())
                .setFileFormat(Database.FileFormat.V2010)
                .create()) {
            Table table = new TableBuilder("Large")
                    .addColumn(new ColumnBuilder("ID", DataType.LONG))
                    .addColumn(new ColumnBuilder("Payload", DataType.OLE))
                    .toTable(db);
            byte[] value = new byte[50 << 20];
            for (int i = 0; i < value.length; i++) {
                value[i] = (byte) (i * 31);
            }
            table.addRow(1, value);
        }
        dumpsWithin(source, "64m");
    }

    private void dumpsWithin(Path source, String heap) throws Exception {
        Path json = dir.resolve("limited.json");
        Path limited = dir.resolve("limited.sql");
        Path unlimited = dir.resolve("unlimited.sql");

        Child jsonRun = convert(heap, "json", source, json);
        assertThat(jsonRun.exitCode()).as(jsonRun.log()).isIn(0, 1);

        Child mysqlRun = convert(heap, "mysql", source, limited);
        assertThat(mysqlRun.log()).doesNotContain("OutOfMemoryError");
        // exit 1: a statement over MariaDB's default max_allowed_packet is a warning
        assertThat(mysqlRun.exitCode()).as(mysqlRun.log()).isIn(0, 1);

        StringWriter err = new StringWriter();
        int code = Main.run(
                new ByteArrayOutputStream(),
                StandardCharsets.UTF_8,
                new PrintWriter(err, true),
                "convert",
                "--to",
                "mysql",
                "--no-report",
                "--no-progress",
                "-o",
                unlimited.toString(),
                source.toString());
        assertThat(code).as(err.toString()).isIn(0, 1);
        assertThat(limited).hasSameBinaryContentAs(unlimited);
    }

    private record Child(int exitCode, String log) {}

    private static Child convert(String heap, String target, Path source, Path output)
            throws IOException, InterruptedException {
        List<String> command = new ArrayList<>(List.of(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-Xmx" + heap,
                "-cp",
                System.getProperty("java.class.path"),
                Main.class.getName(),
                "convert",
                "--to",
                target,
                "--no-report",
                "--no-progress",
                "--overwrite",
                "-o",
                output.toString(),
                source.toString()));
        Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
        String log = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        return new Child(process.waitFor(), log);
    }
}
