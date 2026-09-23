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
