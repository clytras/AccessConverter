package io.lytrax.accessconverter.profile;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.ColumnBuilder;
import com.healthmarketscience.jackcess.DataType;
import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.DatabaseBuilder;
import com.healthmarketscience.jackcess.IndexBuilder;
import com.healthmarketscience.jackcess.PropertyMap;
import com.healthmarketscience.jackcess.RelationshipBuilder;
import com.healthmarketscience.jackcess.Table;
import com.healthmarketscience.jackcess.TableBuilder;
import io.lytrax.accessconverter.Extraction;
import io.lytrax.accessconverter.fixtures.GeneratedFixture;
import io.lytrax.accessconverter.model.SchemaModel;
import io.lytrax.accessconverter.model.TableModel;
import io.lytrax.accessconverter.profile.DataProfile.ColumnStats;
import io.lytrax.accessconverter.report.IssueCode;
import io.lytrax.accessconverter.report.Issues;
import io.lytrax.accessconverter.source.AccessSource;
import io.lytrax.accessconverter.source.SourceException;
import io.lytrax.accessconverter.target.ConvertOptions;
import io.lytrax.accessconverter.target.ConvertOptions.OnTableError;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** 04, Profiling pass: the statistics that decide NOT NULL, CHECKs, exact decimals, sequence seeds and FKs. */
class DataProfilerTest {
    @TempDir
    static Path dir;

    static DataProfile profile;

    @BeforeAll
    static void profileADatabaseWithDirtyData() throws IOException {
        Path file = dir.resolve("dirty.accdb");
        try (Database db = new DatabaseBuilder(file)
                .setFileFormat(Database.FileFormat.V2010)
                .create()) {
            db.setEvaluateExpressions(false);
            Table parent = new TableBuilder("Parent")
                    .addColumn(new ColumnBuilder("Code", DataType.TEXT).setLengthInUnits(10))
                    .addIndex(new IndexBuilder(IndexBuilder.PRIMARY_KEY_NAME)
                            .addColumns("Code")
                            .setPrimaryKey())
                    .toTable(db);
            Table child = new TableBuilder("Child")
                    .addColumn(new ColumnBuilder("ID", DataType.LONG).setAutoNumber(true))
                    .addColumn(new ColumnBuilder("ParentCode", DataType.TEXT).setLengthInUnits(10))
                    .addColumn(new ColumnBuilder("Name", DataType.TEXT).setLengthInUnits(20))
                    .addColumn(
                            new ColumnBuilder("Qty", DataType.INT).putProperty(PropertyMap.VALIDATION_RULE_PROP, ">0"))
                    .addColumn(new ColumnBuilder("Price", DataType.NUMERIC)
                            .setPrecision(28)
                            .setScale(6))
                    .addColumn(new ColumnBuilder("At", DataType.SHORT_DATE_TIME))
                    .addIndex(new IndexBuilder(IndexBuilder.PRIMARY_KEY_NAME)
                            .addColumns("ID")
                            .setPrimaryKey())
                    .toTable(db);
            new RelationshipBuilder("Parent", "Child")
                    .addColumns("Code", "ParentCode")
                    .setReferentialIntegrity()
                    .toRelationship(db);

            parent.addRow("ABC");
            parent.addRow("DEF");
            // Access matches "abc" to "ABC" (case-insensitive), so RI accepts it
            child.addRow(
                    Column.AUTO_NUMBER, "abc", "one", 5, new BigDecimal("1.5"), LocalDateTime.of(2024, 1, 1, 0, 0));
            child.addRow(Column.AUTO_NUMBER, "DEF", null, 0, new BigDecimal("123456789012345678.123"), null);
            child.addRow(Column.AUTO_NUMBER, null, "", null, null, LocalDateTime.of(2024, 1, 1, 0, 0, 0, 250_000_000));
            // A damaged or imported database: an orphan Access would not have accepted
            db.setEnforceForeignKeys(false);
            child.addRow(Column.AUTO_NUMBER, "XYZ", "four", 7, BigDecimal.ZERO, null);
            // Tightened after the fact: Access lets the existing NULL and "" rows stay
            PropertyMap name = child.getColumn("Name").getProperties();
            name.put(PropertyMap.REQUIRED_PROP, true);
            name.put(PropertyMap.ALLOW_ZERO_LEN_PROP, false);
            name.save();
        }
        Extraction extraction = Extraction.of(file);
        try (AccessSource source = AccessSource.open(file)) {
            profile = DataProfiler.profile(source, extraction.model());
        }
    }

    @Test
    void orphansAndInexactMatchesAreCountedThroughTheParentIndex() {
        assertThat(profile.relationship("ParentChild")).hasValueSatisfying(r -> {
            assertThat(r.checked()).isEqualTo(3); // the NULL key is never checked
            assertThat(r.orphans()).isEqualTo(1);
            assertThat(r.orphanSamples()).containsExactly("(4) -> (\"XYZ\")");
            assertThat(r.inexact()).isEqualTo(1);
            assertThat(r.inexactAsciiCaseOnly()).isTrue();
            assertThat(r.inexactSamples()).containsExactly("(1): (\"abc\") matches (\"ABC\")");
        });
    }

    @Test
    void requiredAndZeroLengthColumnsCountNullsAndEmptyStrings() {
        ColumnStats name = column("Name");
        assertThat(name.nulls()).isEqualTo(1);
        assertThat(name.emptyStrings()).isEqualTo(1);
    }

    @Test
    void validationRulesAreCheckedAgainstExistingRows() {
        assertThat(column("Qty").rule()).satisfies(r -> {
            assertThat(r.violations()).isEqualTo(1);
            assertThat(r.samples()).containsExactly("(2)");
            assertThat(r.unevaluable()).isZero();
        });
    }

    @Test
    void decimalsDatesAndAutonumbersRecordWhatTheDataNeeds() {
        assertThat(column("Price").maxSignificantDigits()).isEqualTo(21);
        assertThat(column("Price").maxScale()).isEqualTo(3);
        assertThat(column("At").maxFractionDigits()).isEqualTo(2);
        assertThat(column("ID").maxAutoNumber()).isEqualTo(4);
        assertThat(profile.table("Child").orElseThrow().rowsScanned()).isEqualTo(4);
    }

    @Test
    void theSchemaFidelityFixtureIsClean() throws IOException {
        Extraction extraction = Extraction.of(GeneratedFixture.SCHEMA_FIDELITY.path());
        DataProfile clean;
        try (AccessSource source = AccessSource.open(GeneratedFixture.SCHEMA_FIDELITY.path())) {
            clean = DataProfiler.profile(source, extraction.model());
        }
        assertThat(clean.relationships().values())
                .allSatisfy(r -> assertThat(r.orphans()).isZero());
        assertThat(clean.relationships()).containsKey("CustomersOrders").doesNotContainKey("SuppliersProducts");
        assertThat(clean.table("Customers")
                        .orElseThrow()
                        .column("CustomerID")
                        .orElseThrow()
                        .maxAutoNumber())
                .isEqualTo(2);
        assertThat(clean.table("Order Details")
                        .orElseThrow()
                        .column("Qty")
                        .orElseThrow()
                        .rule()
                        .holds())
                .isTrue();
    }

    /**
     * A parent table that can't be read while its children's keys are checked (04) is the parent's failure, not the
     * child's: under {@code continue} the child is profiled without that relationship, and under {@code fail} the error
     * names the parent. Orders is profiled first here, so its check is what reads Customers.
     */
    @Test
    void aParentThatCannotBeReadFailsItselfNotItsChild() throws IOException {
        SchemaModel model =
                Extraction.of(GeneratedFixture.SCHEMA_FIDELITY.path()).model();
        List<TableModel> ordersFirst = new ArrayList<>(model.tables());
        ordersFirst.sort(Comparator.comparing((TableModel t) -> !t.name().equals("Orders")));
        SchemaModel reordered = new SchemaModel(model.source(), ordersFirst, model.relationships());
        ConvertOptions continueOn =
                new ConvertOptions(true, false, OnTableError.CONTINUE, ConvertOptions.DEFAULT_BATCH_ROWS);

        try (AccessSource source = AccessSource.open(GeneratedFixture.SCHEMA_FIDELITY.path())) {
            FailingProfileSource failing = new FailingProfileSource(source, "Customers");

            Issues continued = new Issues();
            DataProfile profile = DataProfiler.profile(failing, reordered, continueOn, continued);
            assertThat(profile.failedTables()).containsExactly("Customers");
            assertThat(profile.table("Orders")).isPresent();
            assertThat(profile.relationships())
                    .doesNotContainKey("CustomersOrders")
                    .containsKey("OrdersOrder Details");
            assertThat(continued.list())
                    .filteredOn(i -> i.code() == IssueCode.TABLE_READ_FAILED)
                    .singleElement()
                    .satisfies(i -> assertThat(i.table()).isEqualTo("Customers"));

            Issues failed = new Issues();
            assertThatThrownBy(() -> DataProfiler.profile(failing, reordered, ConvertOptions.DEFAULT, failed))
                    .isInstanceOf(SourceException.class)
                    .hasMessageContaining("reading table Customers failed");
            assertThat(failed.list())
                    .filteredOn(i -> i.code() == IssueCode.TABLE_READ_FAILED)
                    .singleElement()
                    .satisfies(i -> assertThat(i.table()).isEqualTo("Customers"));
        }
    }

    private static ColumnStats column(String name) {
        return profile.table("Child").orElseThrow().column(name).orElseThrow();
    }
}
