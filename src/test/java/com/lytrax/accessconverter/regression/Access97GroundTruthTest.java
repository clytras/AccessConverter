package com.lytrax.accessconverter.regression;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

import com.lytrax.accessconverter.Extraction;
import com.lytrax.accessconverter.fixtures.Access97Fixture;
import com.lytrax.accessconverter.fixtures.GuestDump;
import com.lytrax.accessconverter.fixtures.GuestDump.DumpTable;
import com.lytrax.accessconverter.model.AccessType;
import com.lytrax.accessconverter.model.ColumnModel;
import com.lytrax.accessconverter.model.ForeignKeyModel;
import com.lytrax.accessconverter.model.TableModel;
import com.lytrax.accessconverter.source.AccessSource;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * gr97.mdb read against the dump Access 97 itself printed for it in the Windows 98 guest. It is a Greek Access 97
 * file, so it covers both of the Access 97 findings at once: the catalog index Jackcess can't use, and text in
 * code page 1253.
 */
class Access97GroundTruthTest {
    private static final Path FILE = Access97Fixture.GR97.file();
    private static final GuestDump DUMP = Access97Fixture.GR97.dump();
    /** How Access 97 prints True and False in Greek. */
    private static final String TRUE = "Αληθές";

    private static final String FALSE = "Ψευδές";

    @Test
    void theTablesColumnsAndRowCountsAreWhatAccessShows() {
        Extraction extraction = Extraction.of(FILE);
        assertThat(extraction.model().tables())
                .extracting(TableModel::name)
                .containsExactlyElementsOf(
                        DUMP.tables().stream().map(DumpTable::name).toList());
        for (DumpTable dumped : DUMP.tables()) {
            TableModel table = extraction.table(dumped.name());
            assertThat(table.columns())
                    .as(dumped.name())
                    .extracting(ColumnModel::name)
                    .containsExactlyElementsOf(dumped.fields());
            assertThat(dumped.rows()).as("%s rows dumped", dumped.name()).hasSize(dumped.rowCount());
        }
    }

    /** Every value of every row, against the dump: NULL, "", Greek and ASCII text, numbers, booleans and dates. */
    @ParameterizedTest
    @ValueSource(strings = {"AllTypes", "Customers", "Orders"})
    void everyValueMatchesWhatAccessPrinted(String tableName) throws IOException {
        DumpTable dumped = DUMP.table(tableName);
        TableModel table = Extraction.of(FILE).table(tableName);
        List<Object[]> rows = byId(rows(table));
        List<List<String>> dumpedRows = dumped.rows().stream()
                .sorted(Comparator.comparingLong(row -> Long.parseLong(row.get(0))))
                .toList();
        assertThat(rows).as(tableName).hasSize(dumped.rowCount());
        for (int r = 0; r < rows.size(); r++) {
            for (int c = 0; c < table.columns().size(); c++) {
                ColumnModel column = table.columns().get(c);
                String cell = dumpedRows.get(r).get(c);
                assertValue(rows.get(r)[c], cell, column.type(), tableName + "." + column.name() + " row " + (r + 1));
            }
        }
    }

    @Test
    void theNumericExtremesSurvive() throws IOException {
        // Row 2 of AllTypes holds each type's limit, as the dump shows it: 255, -32768, 2147483647, -3,4E+38 ...
        Object[] limits = rows(Extraction.of(FILE).table("AllTypes")).get(1);
        assertThat(limits[1]).isEqualTo(255); // Byte: unsigned in Access, so an Integer here (F-01)
        assertThat(limits[2]).isEqualTo((short) -32768);
        assertThat(limits[3]).isEqualTo(2147483647);
        assertThat((Float) limits[4]).isEqualTo(-3.4E38f);
        // Access parsed "1.7E308" into the double next to Java's literal, one unit in the last place away
        assertThat((Double) limits[5]).isCloseTo(1.7E308, within(Math.ulp(1.7E308)));
        assertThat(limits[6]).isEqualTo(new BigDecimal("-922337203685477.5808")); // Currency's lowest value
    }

    @Test
    void theMemoKeepsItsTabsNewlinesQuotesAndBackslash() throws IOException {
        Object[] row = rows(Extraction.of(FILE).table("AllTypes")).get(2);
        assertThat((String) row[10]).isEqualTo("tabs\tnewlines\r\nquotes \" and , and ; and \\");
        assertThat((String) row[9]).isEqualTo("Ελληνικά: ΑΒΓΔΕΖΗΘ αβγδεζηθ ςΆΈΉΊΌΎΏ");
    }

    @Test
    void nullIsNotTheEmptyString() throws IOException {
        List<Object[]> all = rows(Extraction.of(FILE).table("AllTypes"));
        assertThat(all.get(3)[9]).as("NULL text").isNull();
        assertThat(all.get(3)[10]).as("NULL memo").isNull();
        assertThat(all.get(4)[9]).as("zero-length text").isEqualTo("");
        assertThat(all.get(4)[10]).as("zero-length memo").isEqualTo("");
    }

    @Test
    void theRelationshipIsTheOneAccessShows() {
        GuestDump.DumpRelation dumped = DUMP.relation("CustomersOrders").orElseThrow();
        ForeignKeyModel relationship = Extraction.of(FILE).relationship(dumped.name());
        assertThat(relationship.parentTable()).isEqualTo(dumped.parent());
        assertThat(relationship.childTable()).isEqualTo(dumped.child());
        assertThat(relationship.parentColumns()).containsExactly(dumped.parentColumn());
        assertThat(relationship.childColumns()).containsExactly(dumped.childColumn());
    }

    /**
     * The dumper prints what Access reads, in Greek and in the guest's formats: decimal comma, day/month/year, and
     * long values cut short with "...". What it can be compared with exactly is text, integers and Currency.
     */
    private static void assertValue(Object value, String cell, AccessType type, String where) {
        String expected = GuestDump.value(cell);
        if (expected == null) {
            assertThat(value).as(where).isNull();
            return;
        }
        assertThat(value).as(where).isNotNull();
        if (cell.startsWith("<binary ")) {
            return; // OLE: the dumper prints its size only
        }
        switch (type) {
            case TEXT, MEMO -> {
                if (cell.endsWith(GuestDump.TRUNCATED)) {
                    assertThat((String) value)
                            .as(where)
                            .startsWith(expected.substring(0, expected.length() - GuestDump.TRUNCATED.length()));
                } else {
                    assertThat(value).as(where).isEqualTo(expected);
                }
            }
            case BOOLEAN -> {
                assertThat(expected).as(where).isIn(TRUE, FALSE);
                assertThat(value).as(where).isEqualTo(TRUE.equals(expected));
            }
            case BYTE, INT, LONG -> assertThat(value.toString()).as(where).isEqualTo(expected);
            case MONEY ->
                assertThat((BigDecimal) value)
                        .as(where)
                        .isEqualByComparingTo(new BigDecimal(expected.replace(',', '.')));
            case FLOAT ->
                assertThat((Float) value)
                        .as(where)
                        .isCloseTo(
                                Float.parseFloat(expected.replace(',', '.')), within(Math.abs((Float) value) * 1e-6f));
            case DOUBLE ->
                assertThat((Double) value)
                        .as(where)
                        .isCloseTo(
                                Double.parseDouble(expected.replace(',', '.')),
                                within(Math.abs((Double) value) * 1e-6));
            case SHORT_DATE_TIME -> assertThat(value).as(where).isInstanceOf(LocalDateTime.class);
            default -> {}
        }
    }

    /**
     * Rows by their leading id column. Access hands rows out in the order of whichever index it used, so the
     * dumper printed Orders by CustomerID, while a table scan here gives them in the order they were written.
     */
    private static List<Object[]> byId(List<Object[]> rows) {
        return rows.stream()
                .sorted(Comparator.comparingLong(row -> ((Number) row[0]).longValue()))
                .toList();
    }

    private static List<Object[]> rows(TableModel table) throws IOException {
        List<Object[]> rows = new ArrayList<>();
        try (AccessSource source = AccessSource.open(FILE)) {
            source.scan(table, table.columns()).forEachRemaining(rows::add);
        }
        return rows;
    }
}
