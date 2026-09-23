package com.lytrax.accessconverter.regression;

import static org.assertj.core.api.Assertions.assertThat;

import com.lytrax.accessconverter.Extraction;
import com.lytrax.accessconverter.fixtures.Access97Fixture;
import com.lytrax.accessconverter.fixtures.CorpusCase;
import com.lytrax.accessconverter.fixtures.CorpusFile;
import com.lytrax.accessconverter.fixtures.LocalSample;
import com.lytrax.accessconverter.fixtures.LocalSamples;
import com.lytrax.accessconverter.model.ForeignKeyModel;
import com.lytrax.accessconverter.model.ForeignKeyModel.Action;
import com.lytrax.accessconverter.model.TableModel;
import com.lytrax.accessconverter.report.Issue;
import com.lytrax.accessconverter.report.IssueCode;
import com.lytrax.accessconverter.report.Issues;
import com.lytrax.accessconverter.source.AccessSource;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Jackcess 5.0.1 can't use the {@code MSysObjects} index of databases written by a non-English Access: tables and
 * the system tables then look absent, and a conversion would quietly leave them out (Northwind from a Greek Access
 * 97: 3 of 8 tables, none of the 7 relationships). The source layer reads such a catalog by scanning it instead.
 */
class CatalogIndexTest {
    private static final Set<IssueCode> CATALOG_ISSUES = Set.of(
            IssueCode.CATALOG_INDEX_UNUSABLE, IssueCode.TABLE_NOT_IN_CATALOG, IssueCode.RELATIONSHIPS_UNREADABLE);

    @Test
    void aGreekAccessNinetySevenDatabaseIsReadWhole() {
        Extraction extraction = Extraction.of(Access97Fixture.GR97.file());
        assertThat(extraction.model().tables())
                .extracting(TableModel::name)
                .containsExactly("AllTypes", "Customers", "Orders");
        assertThat(extraction.model().relationships())
                .extracting(ForeignKeyModel::name)
                .containsExactly("CustomersOrders");
        assertThat(extraction.issues(IssueCode.CATALOG_INDEX_UNUSABLE))
                .singleElement()
                .satisfies(i -> assertThat(i.message())
                        .contains("MSysRelationships is invisible")
                        .contains("scanning"));
        // Nothing was left out, so nothing is reported as lost
        assertThat(extraction.issues()).extracting(Issue::code).doesNotContain(IssueCode.TABLE_NOT_IN_CATALOG);
    }

    /**
     * The fallback opens the database a second time, so it has to carry everything the first open had: an encoded
     * Access 97 database (readable only through the codec provider) whose catalog index is unusable and which also
     * has a database password. Dropping either on the way would fail the open or lose the tables again.
     */
    @Test
    void anEncodedAndPasswordProtectedDatabaseSurvivesTheFallback() {
        Access97Fixture fixture = Access97Fixture.GR97_ENC;
        Extraction extraction = Extraction.of(fixture.file(), fixture.openOptions());
        assertThat(extraction.model().tables())
                .extracting(TableModel::name)
                .containsExactly("AllTypes", "Customers", "Orders");
        assertThat(extraction.model().relationships()).hasSize(1);
        assertThat(extraction.issues())
                .extracting(Issue::code)
                .contains(IssueCode.CATALOG_INDEX_UNUSABLE, IssueCode.PASSWORD_NOT_REQUIRED)
                .doesNotContain(IssueCode.TABLE_NOT_IN_CATALOG, IssueCode.RELATIONSHIPS_UNREADABLE);
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

    /** Tiers A and B: no other file's catalog needs the fallback, and none hides a table. */
    @ParameterizedTest(name = "{0}")
    @MethodSource("databases")
    void everyOtherCatalogResolvesOnItsOwn(CorpusCase database) {
        assertThat(Extraction.of(database.file(), database.options()).issues())
                .extracting(Issue::code)
                .doesNotContainAnyElementsOf(CATALOG_ISSUES);
    }

    /** Tier D's Greek Access 97 file is the known-broken one, checked above. */
    static Stream<CorpusCase> databases() {
        return CorpusCase.databases().filter(c -> !c.id().startsWith("access97/"));
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

        /** Northwind as Greek Access 97 wrote it: what Jackcess alone reads is 3 tables and no relationship. */
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
            assertThat(extraction.issues(IssueCode.CATALOG_INDEX_UNUSABLE))
                    .singleElement()
                    .satisfies(i -> assertThat(i.message()).contains("5 of 8 tables"));
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
                        .doesNotContain(IssueCode.TABLE_NOT_IN_CATALOG, IssueCode.RELATIONSHIPS_UNREADABLE);
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
