package com.lytrax.accessconverter.fixtures;

import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.ColumnBuilder;
import com.healthmarketscience.jackcess.DataType;
import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.IndexBuilder;
import com.healthmarketscience.jackcess.Table;
import com.healthmarketscience.jackcess.TableBuilder;
import java.io.IOException;

/** Builds {@code textEdge.accdb} (audit tool {@code MakeTextEdge}): strings that stress SQL literal escaping (F-02). */
final class TextEdgeFixture {
    private TextEdgeFixture() {}

    static void build(Database db) throws IOException {
        Table t = new TableBuilder("TextEdge")
                .addColumn(new ColumnBuilder("ID", DataType.LONG).setAutoNumber(true))
                .addColumn(new ColumnBuilder("Label", DataType.TEXT).setLengthInUnits(30))
                .addColumn(new ColumnBuilder("Val", DataType.TEXT).setLengthInUnits(255))
                .addIndex(new IndexBuilder(IndexBuilder.PRIMARY_KEY_NAME)
                        .addColumns("ID")
                        .setPrimaryKey())
                .toTable(db);
        t.addRow(Column.AUTO_NUMBER, "windows-path", "C:\\temp\\new");
        t.addRow(Column.AUTO_NUMBER, "trailing-backslash", "ends with backslash\\");
        t.addRow(Column.AUTO_NUMBER, "quotes", "single ' and double \"");
        t.addRow(Column.AUTO_NUMBER, "newline", "line1\nline2");
        t.addRow(Column.AUTO_NUMBER, "emoji", "emoji \uD83D\uDE00 and \u03a9\u03bc\u03ad\u03b3\u03b1");
        t.addRow(Column.AUTO_NUMBER, "ctrl-z", "a\u001Ab");
        // A value ending in a backslash swallowed the rest of v2's INSERT batch; this row must survive it
        t.addRow(Column.AUTO_NUMBER, "after", "row after the tricky ones");
    }
}
