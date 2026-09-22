package com.lytrax.accessconverter.fixtures;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

import com.healthmarketscience.jackcess.CursorBuilder;
import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.Relationship;
import com.healthmarketscience.jackcess.Row;
import com.healthmarketscience.jackcess.Table;
import com.lytrax.accessconverter.fixtures.UniqueProbeFixture.Pair;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/** Tier A smoke test: every generated fixture builds and holds the values the audit (01) relied on. */
class GeneratedFixtureTest {

    @Test
    void schemaFidelityHasTheAuditedShape() throws IOException {
        try (Database db = GeneratedFixture.SCHEMA_FIDELITY.open()) {
            assertThat(localTableNames(db))
                    .hasSize(11)
                    .contains("Customers", "Order Details", "Shippers", "WideTable", "Πελάτες");
            assertThat(db.getRelationships())
                    .extracting(Relationship::getName, Relationship::hasReferentialIntegrity)
                    .hasSize(6)
                    .contains(tuple("CustomersOrders", true), tuple("SuppliersProducts", false));
            assertThat(db.getTable("WideTable").getColumnCount()).isEqualTo(71);
        }
    }

    @Test
    void schemaFidelityKeepsTheAwkwardValues() throws IOException {
        try (Database db = GeneratedFixture.SCHEMA_FIDELITY.open()) {
            Table customers = db.getTable("Customers");
            Row alice = CursorBuilder.findRowByPrimaryKey(customers, -5);

            // Jackcess hands Access's unsigned Byte back as a signed byte: 200 reads as -56 (F-01)
            assertThat(alice.get("Rating")).isEqualTo((byte) 200);
            assertThat(alice.getBigDecimal("Balance")).isEqualByComparingTo("123456789012345.1234");
            assertThat(alice.getBigDecimal("Discount")).isEqualByComparingTo("12.3456");
            assertThat(alice.getString("Notes")).hasSize(70_000);
            // LocalDateTime in, LocalDateTime out: the same in every surefire time zone
            assertThat(alice.getLocalDateTime("Created")).isEqualTo(LocalDateTime.of(2024, 1, 2, 3, 4, 5));

            List<String> codes = new ArrayList<>();
            for (Row row : customers) {
                codes.add(row.getString("Code"));
            }
            assertThat(codes).containsExactlyInAnyOrder("C1", "resume", "résumé");
        }
    }

    @Test
    void textEdgeKeepsEveryString() throws IOException {
        try (Database db = GeneratedFixture.TEXT_EDGE.open()) {
            Table t = db.getTable("TextEdge");
            assertThat(t.getRowCount()).isEqualTo(7);
            assertThat(CursorBuilder.findRowByPrimaryKey(t, 1).getString("Val")).isEqualTo("C:\\temp\\new");
        }
    }

    @Test
    void hundredRowsIsExactlyOneV2Batch() throws IOException {
        try (Database db = GeneratedFixture.HUNDRED_ROWS.open()) {
            assertThat(db.getTable("Hundred").getRowCount()).isEqualTo(HundredRowsFixture.ROWS);
            assertThat(db.getTable("After").getRowCount()).isEqualTo(1);
        }
    }

    /** 01, "Access unique-index semantics": accent variants are distinct; case, trailing spaces and ß/ss are equal. */
    @ParameterizedTest
    @EnumSource(Pair.class)
    void uniqueProbeRecordsAccessEquality(Pair pair) throws IOException {
        boolean distinctInAccess = pair == Pair.ACCENT_VARIANTS;
        try (Database db = GeneratedFixture.UNIQUE_PROBE.open()) {
            assertThat(db.getTable(pair.tableName()).getRowCount()).isEqualTo(distinctInAccess ? 2 : 1);
        }
    }

    static List<String> localTableNames(Database db) {
        List<String> names = new ArrayList<>();
        for (Table t : db.newIterable().withLocalUserTablesOnly()) {
            names.add(t.getName());
        }
        return names;
    }
}
