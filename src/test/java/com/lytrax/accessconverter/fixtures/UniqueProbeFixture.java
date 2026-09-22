package com.lytrax.accessconverter.fixtures;

import com.healthmarketscience.jackcess.ColumnBuilder;
import com.healthmarketscience.jackcess.ConstraintViolationException;
import com.healthmarketscience.jackcess.DataType;
import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.IndexBuilder;
import com.healthmarketscience.jackcess.Table;
import com.healthmarketscience.jackcess.TableBuilder;
import java.io.IOException;
import java.util.Locale;

/**
 * Builds {@code uniqueProbe.accdb} (audit tool {@code UniqProbe}): one table per {@link Pair}, with a unique index
 * on {@code v}. Each table holds the pair's first value, plus the second one when Access (Jackcess's General sort
 * order) keeps the two distinct. A target collation is never stricter than Access (05) only if every table imports.
 */
public final class UniqueProbeFixture {
    public static final String COLUMN = "v";

    /** The collation probe pairs of 05. */
    public enum Pair {
        ACCENT_VARIANTS("resume", "résumé"),
        TRAILING_SPACE("abc", "abc "),
        ESZETT_SS("straße", "strasse"),
        CASE_VARIANTS("Code", "CODE");

        private final String first;
        private final String second;

        Pair(String first, String second) {
            this.first = first;
            this.second = second;
        }

        public String first() {
            return first;
        }

        public String second() {
            return second;
        }

        public String tableName() {
            return "T_" + name().toLowerCase(Locale.ROOT);
        }
    }

    private UniqueProbeFixture() {}

    static void build(Database db) throws IOException {
        for (Pair pair : Pair.values()) {
            Table t = new TableBuilder(pair.tableName())
                    .addColumn(new ColumnBuilder(COLUMN, DataType.TEXT).setLengthInUnits(20))
                    .addIndex(new IndexBuilder("u").addColumns(COLUMN).setUnique())
                    .toTable(db);
            t.addRow(pair.first);
            try {
                t.addRow(pair.second);
            } catch (ConstraintViolationException equalInAccess) {
                // Access treats the pair as one key: the table keeps only the first value
            }
        }
    }
}
