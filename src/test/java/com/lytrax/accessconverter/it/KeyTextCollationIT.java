package com.lytrax.accessconverter.it;

import static org.assertj.core.api.Assertions.assertThat;

import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.ColumnBuilder;
import com.healthmarketscience.jackcess.ConstraintViolationException;
import com.healthmarketscience.jackcess.DataType;
import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.DatabaseBuilder;
import com.healthmarketscience.jackcess.IndexBuilder;
import com.healthmarketscience.jackcess.Row;
import com.healthmarketscience.jackcess.Table;
import com.healthmarketscience.jackcess.TableBuilder;
import com.lytrax.accessconverter.fixtures.CorpusFile;
import com.lytrax.accessconverter.profile.KeyText;
import com.lytrax.accessconverter.report.IssueCode;
import com.lytrax.accessconverter.target.mysql.MySqlFixture;
import com.lytrax.accessconverter.target.mysql.MySqlFixture.Converted;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * F-36 on every server (09): a unique key that holds in Access holds in the output. Every text value of
 * {@code indexCodesV2010}, which covers every character class Access sorts, goes into two Access tables with a
 * unique index, keeping only what Access itself accepts as distinct: {@code SafeKeys} with the values made of
 * characters {@link KeyText} calls safe, which keep the default collation, and {@code AllKeys} with all of them, which
 * turns to {@code utf8mb4_bin}. Both must import, and verify.
 */
class KeyTextCollationIT {

    @TempDir
    static Path dir;

    private static Path keys;

    static Stream<String> images() {
        return DatabaseServer.images().stream();
    }

    @BeforeAll
    static void build() throws IOException {
        Set<String> values = new LinkedHashSet<>();
        try (Database source = DatabaseBuilder.open(
                CorpusFile.get("jackcess/V2010/indexCodesV2010.accdb").file().toFile())) {
            for (Table table : source) {
                for (Row row : table) {
                    for (Column column : table.getColumns()) {
                        if (column.getType() == DataType.TEXT
                                && row.get(column.getName()) instanceof String s
                                && !s.isEmpty()
                                && s.length() <= 100) {
                            values.add(s);
                        }
                    }
                }
            }
        }
        keys = dir.resolve("keys.accdb");
        try (Database db = new DatabaseBuilder(keys.toFile())
                .setFileFormat(Database.FileFormat.V2010)
                .create()) {
            Table safe = keyTable(db, "SafeKeys");
            Table all = keyTable(db, "AllKeys");
            for (String value : values) {
                if (KeyText.isSafe(value)) {
                    addIfDistinct(safe, value);
                }
                addIfDistinct(all, value);
            }
        }
    }

    private static Table keyTable(Database db, String name) throws IOException {
        return new TableBuilder(name)
                .addColumn(new ColumnBuilder("v", DataType.TEXT).setLengthInUnits(100))
                .addIndex(new IndexBuilder("PrimaryKey").addColumns("v").setPrimaryKey())
                .toTable(db);
    }

    /** Keeps the value only when Access's own index takes it for a new key. */
    private static void addIfDistinct(Table table, String value) throws IOException {
        try {
            table.addRow(value);
        } catch (ConstraintViolationException sameKeyInAccess) {
            // Access holds an equal key already: it would never be in such a table twice
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("images")
    void everyKeyAccessHoldsDistinctStaysDistinct(String image) throws Exception {
        DatabaseServer server = DatabaseServer.of(image);
        Path dump = dir.resolve(image.replace(':', '-') + "-keys.sql");
        Converted converted = MySqlFixture.convert(keys, dump, server.dialect());

        assertThat(converted.column("SafeKeys", "v").collation()).isNull();
        assertThat(converted.column("AllKeys", "v").collation()).isEqualTo("utf8mb4_bin");
        assertThat(converted.issues(IssueCode.COLLATION_BINARY_KEY))
                .singleElement()
                .satisfies(i -> assertThat(i.table()).isEqualTo("AllKeys"));
        server.recreate("keys");
        assertThat(server.importDump(dump, "keys")).isEmpty();
        try (Connection db = server.connect("keys")) {
            assertThat(converted.verify(db).differences()).isEmpty();
        }
    }
}
