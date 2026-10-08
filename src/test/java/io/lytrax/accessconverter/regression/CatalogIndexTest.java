package io.lytrax.accessconverter.regression;

import static org.assertj.core.api.Assertions.assertThat;

import io.lytrax.accessconverter.Extraction;
import io.lytrax.accessconverter.fixtures.Access97Fixture;
import io.lytrax.accessconverter.fixtures.CorpusCase;
import io.lytrax.accessconverter.fixtures.CorpusFile;
import io.lytrax.accessconverter.fixtures.DamagedCopy;
import io.lytrax.accessconverter.fixtures.GeneratedFixture;
import io.lytrax.accessconverter.fixtures.LocalSample;
import io.lytrax.accessconverter.fixtures.LocalSamples;
import io.lytrax.accessconverter.model.ForeignKeyModel;
import io.lytrax.accessconverter.model.ForeignKeyModel.Action;
import io.lytrax.accessconverter.model.TableModel;
import io.lytrax.accessconverter.report.Issue;
import io.lytrax.accessconverter.report.IssueCode;
import io.lytrax.accessconverter.report.Issues;
import io.lytrax.accessconverter.source.AccessSource;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Jackcess 5.0.1 couldn't use the {@code MSysObjects} index of databases written by a non-English Access: tables and
 * the system tables then looked absent, and a conversion would quietly have left them out (Northwind from a Greek
 * Access 97: 3 of 8 tables, none of the 7 relationships). Jackcess 5.0.2 scans such a catalog itself. The source
 * layer still reads the catalog by scanning it when the index misses part of it, which now takes a damaged index:
 * those tests damage one entry of an English file's catalog index ({@link DamagedCopy}).
 */
class CatalogIndexTest {
    private static final Set<IssueCode> CATALOG_ISSUES = Set.of(
            IssueCode.CATALOG_INDEX_UNUSABLE, IssueCode.TABLE_NOT_IN_CATALOG, IssueCode.RELATIONSHIPS_UNREADABLE);

    @TempDir
    Path dir;

    @Test
    void aGreekAccessNinetySevenDatabaseIsReadWhole() {
        Extraction extraction = Extraction.of(Access97Fixture.GR97.file());
        assertThat(extraction.model().tables())
                .extracting(TableModel::name)
                .containsExactly("AllTypes", "Customers", "Orders");
        assertThat(extraction.model().relationships())
                .extracting(ForeignKeyModel::name)
                .containsExactly("CustomersOrders");
        assertThat(extraction.issues()).extracting(Issue::code).doesNotContainAnyElementsOf(CATALOG_ISSUES);
    }

    /** The encoded copy, which also has a database password, is read whole too. */
    @Test
    void anEncodedAndPasswordProtectedGreekDatabaseIsReadWhole() {
        Access97Fixture fixture = Access97Fixture.GR97_ENC;
        Extraction extraction = Extraction.of(fixture.file(), fixture.openOptions());
        assertThat(extraction.model().tables())
                .extracting(TableModel::name)
                .containsExactly("AllTypes", "Customers", "Orders");
        assertThat(extraction.model().relationships()).hasSize(1);
        assertThat(extraction.issues())
                .extracting(Issue::code)
                .contains(IssueCode.PASSWORD_NOT_REQUIRED)
                .doesNotContainAnyElementsOf(CATALOG_ISSUES);
    }

    /**
     * A table the catalog index can't find is read by scanning the catalog. The fallback opens the database a second
     * time, so it has to carry everything the first open had: the encoded files are readable only through the codec
     * provider. An encrypted (AES) file can't be damaged this way: one flipped bit garbles a whole block of the page,
     * the entries fall out of order, and Jackcess then fails the open, which is reported as a damaged file.
     */
    @ParameterizedTest(name = "{0}")
    @CsvSource({
        "jackcess/V1997/common1V1997.mdb, Table1, 4",
        "jackcess/V2003/delV2003.mdb, Table, 1",
        "jackcess-encrypt/db97-enc.mdb, Table1, 1",
        "jackcess-encrypt/db-enc.mdb, Table1, 1"
    })
    void aTableTheDamagedCatalogIndexMissesIsReadByScanning(String id, String table, int tables) throws IOException {
        Path damaged = DamagedCopy.withCatalogIndexDamaged(
                CorpusFile.get(id).file(), null, table, dir.resolve(Path.of(id).getFileName()));
        Extraction extraction = Extraction.of(damaged);
        assertThat(extraction.model().tables()).extracting(TableModel::name).contains(table);
        assertThat(extraction.issues(IssueCode.CATALOG_INDEX_UNUSABLE))
                .singleElement()
                .satisfies(i -> assertThat(i.message())
                        .contains("1 of " + tables + " tables can't be looked up")
                        .contains("scanning"));
        assertThat(extraction.issues())
                .extracting(Issue::code)
                .doesNotContain(IssueCode.TABLE_NOT_IN_CATALOG, IssueCode.RELATIONSHIPS_UNREADABLE);
    }

    /** A relationship table the catalog index can't find is read by scanning too, with every relationship. */
    @Test
    void anInvisibleRelationshipTableIsReadByScanning() throws IOException {
        Path source = GeneratedFixture.SCHEMA_FIDELITY.path();
        Path damaged = DamagedCopy.withCatalogIndexDamaged(
                source, null, AccessSource.RELATIONSHIPS_TABLE, dir.resolve("schemaFidelity.accdb"));
        Extraction extraction = Extraction.of(damaged);
        assertThat(extraction.model().relationships())
                .hasSize(6)
                .extracting(ForeignKeyModel::name)
                .containsExactlyElementsOf(Extraction.of(source).model().relationships().stream()
                        .map(ForeignKeyModel::name)
                        .toList());
        assertThat(extraction.issues(IssueCode.CATALOG_INDEX_UNUSABLE))
                .singleElement()
                .satisfies(i -> assertThat(i.message()).contains("MSysRelationships is invisible"));
        assertThat(extraction.issues()).extracting(Issue::code).doesNotContain(IssueCode.RELATIONSHIPS_UNREADABLE);
    }

    /** Its data is the data of the plain fixture, so the encoded pages decoded correctly after the second open. */
    @Test
    void theEncodedDatabaseHoldsTheSameRowsAsThePlainOne() throws IOException {
        Access97Fixture fixture = Access97Fixture.GR97_ENC;
        TableModel table = Extraction.of(fixture.file(), fixture.openOptions()).table("Customers");
        List<Object[]> rows = new ArrayList<>();
        try (AccessSource source = AccessSource.open(fixture.file(), fixture.openOptions(), new Issues())) {
            source.scan(table, table.columns()).forEachRemaining(rows::add);
        }
        List<String> names = rows.stream().map(row -> (String) row[1]).toList();
        assertThat(names)
                .containsExactlyElementsOf(fixture.dump().table("Customers").column("CompanyName"));
        assertThat(names).contains("Αφοι Παπαδοπούλου ΑΕ");
    }

    /** Tiers A, B and D: no file's catalog needs the fallback, the Greek Access 97 ones included. */
    @ParameterizedTest(name = "{0}")
    @MethodSource("databases")
    void everyCatalogResolvesOnItsOwn(CorpusCase database) {
        assertThat(Extraction.of(database.file(), database.options()).issues())
                .extracting(Issue::code)
                .doesNotContainAnyElementsOf(CATALOG_ISSUES);
    }

    static Stream<CorpusCase> databases() {
        return CorpusCase.databases();
    }

    /** A text collation Jackcess can't index (04) affects table indexes, not the catalog. */
    @Test
    void aJetFourFileWithAnUnusualSortOrderKeepsItsCatalog() {
        Extraction extraction = Extraction.of(CorpusFile.get("jackcess/other/unsupportedSortOrder.accdb"));
        assertThat(extraction.model().tables()).isNotEmpty();
        assertThat(extraction.issues()).extracting(Issue::code).doesNotContainAnyElementsOf(CATALOG_ISSUES);
    }

    @Nested
    @LocalSamples
    class Samples {

        /** Northwind as Greek Access 97 wrote it: what Jackcess 5.0.1 read was 3 tables and no relationship. */
        @Test
        void northwindKeepsAllItsTablesAndRelationships() {
            Extraction extraction = Extraction.of(LocalSample.NORTHWIND_97.path());
            assertThat(extraction.model().tables())
                    .extracting(TableModel::name)
                    .containsExactly(
                            "Categories",
                            "Customers",
                            "Employees",
                            "Order Details",
                            "Orders",
                            "Products",
                            "Shippers",
                            "Suppliers");
            List<ForeignKeyModel> relationships = extraction.model().relationships();
            assertThat(relationships)
                    .hasSize(7)
                    .allSatisfy(r -> assertThat(r.enforced()).isTrue());
            assertThat(relationship(relationships, "CustomersOrders").onUpdate())
                    .isEqualTo(Action.CASCADE);
            assertThat(relationship(relationships, "OrdersOrder Details").onDelete())
                    .isEqualTo(Action.CASCADE);
            assertThat(extraction.issues()).extracting(Issue::code).doesNotContainAnyElementsOf(CATALOG_ISSUES);
        }

        @Test
        void theTablesMatchTheDumpAccessItselfPrinted() {
            Extraction extraction = Extraction.of(LocalSample.NORTHWIND_97.path());
            LocalSample.NORTHWIND_97
                    .dump()
                    .tables()
                    .forEach(
                            dumped -> assertThat(extraction.table(dumped.name()).columns())
                                    .as(dumped.name())
                                    .extracting(c -> c.name())
                                    .containsExactlyElementsOf(dumped.fields()));
        }

        @Test
        void theOtherWindowsNinetyEightSamplesResolveToo() {
            for (LocalSample sample : List.of(LocalSample.SOLUTIONS_97, LocalSample.ORDERS_97)) {
                Extraction extraction = Extraction.of(sample.path());
                assertThat(extraction.issues())
                        .as(sample.name())
                        .extracting(Issue::code)
                        .doesNotContainAnyElementsOf(CATALOG_ISSUES);
            }
        }
    }

    private static ForeignKeyModel relationship(List<ForeignKeyModel> relationships, String name) {
        return relationships.stream()
                .filter(r -> r.name().equals(name))
                .findFirst()
                .orElseThrow(() -> new AssertionError("no relationship " + name + " in " + relationships));
    }
}
