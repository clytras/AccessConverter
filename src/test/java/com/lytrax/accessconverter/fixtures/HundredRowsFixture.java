package com.lytrax.accessconverter.fixtures;

import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.ColumnBuilder;
import com.healthmarketscience.jackcess.DataType;
import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.IndexBuilder;
import com.healthmarketscience.jackcess.Table;
import com.healthmarketscience.jackcess.TableBuilder;
import java.io.IOException;

/**
 * Builds {@code hundred.accdb} (audit tool {@code Make100}): a table with exactly 100 rows, followed by another
 * table. v2 left a dangling {@code INSERT ... VALUES} at the 100-row batch boundary, which swallowed the next
 * statement (F-19).
 */
final class HundredRowsFixture {
    static final int ROWS = 100;

    private HundredRowsFixture() {}

    static void build(Database db) throws IOException {
        Table hundred = new TableBuilder("Hundred")
                .addColumn(new ColumnBuilder("ID", DataType.LONG).setAutoNumber(true))
                .addIndex(new IndexBuilder(IndexBuilder.PRIMARY_KEY_NAME)
                        .addColumns("ID")
                        .setPrimaryKey())
                .toTable(db);
        for (int i = 0; i < ROWS; i++) {
            hundred.addRow(Column.AUTO_NUMBER);
        }
        Table after = new TableBuilder("After")
                .addColumn(new ColumnBuilder("ID", DataType.LONG))
                .toTable(db);
        after.addRow(1);
    }
}
