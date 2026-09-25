package io.lytrax.accessconverter.fixtures;

import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.ColumnBuilder;
import com.healthmarketscience.jackcess.DataType;
import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.IndexBuilder;
import com.healthmarketscience.jackcess.Table;
import com.healthmarketscience.jackcess.TableBuilder;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

/** The performance tests' database (09): one table {@code Large} of a million rows of mixed types. */
public final class LargeFixture {

    public static final int ROWS = 1_000_000;

    private LargeFixture() {}

    /** A million rows of mixed types, built once per machine and kept in {@code target/fixtures}. */
    public static synchronized Path path() throws IOException {
        Path file = Fixtures.root().resolve("generated").resolve("large.accdb");
        if (Files.isRegularFile(file)) {
            return file;
        }
        Files.createDirectories(file.getParent());
        Path partial = file.resolveSibling("large.accdb.partial");
        Files.deleteIfExists(partial);
        try (Database db = new com.healthmarketscience.jackcess.DatabaseBuilder(partial)
                .setFileFormat(Database.FileFormat.V2010)
                .create()) {
            db.setDateTimeType(com.healthmarketscience.jackcess.DateTimeType.LOCAL_DATE_TIME);
            db.setEvaluateExpressions(false);
            Table table = new TableBuilder("Large")
                    .addColumn(new ColumnBuilder("ID", DataType.LONG).setAutoNumber(true))
                    .addColumn(new ColumnBuilder("Name", DataType.TEXT).setLengthInUnits(50))
                    .addColumn(new ColumnBuilder("Amount", DataType.MONEY))
                    .addColumn(new ColumnBuilder("When", DataType.SHORT_DATE_TIME))
                    .addColumn(new ColumnBuilder("Flag", DataType.BOOLEAN))
                    .addIndex(new IndexBuilder(IndexBuilder.PRIMARY_KEY_NAME)
                            .addColumns("ID")
                            .setPrimaryKey())
                    .toTable(db);
            LocalDateTime start = LocalDateTime.of(2000, 1, 1, 0, 0);
            List<Object[]> batch = new ArrayList<>(1000);
            for (int i = 0; i < ROWS; i++) {
                batch.add(new Object[] {
                    Column.AUTO_NUMBER, "row " + i, BigDecimal.valueOf(i % 100_000, 2), start.plusMinutes(i), i % 2 == 0
                });
                if (batch.size() == 1000) {
                    table.addRows(batch);
                    batch.clear();
                }
            }
            if (!batch.isEmpty()) {
                table.addRows(batch);
            }
        }
        return Files.move(partial, file);
    }
}
