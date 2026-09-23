package com.lytrax.accessconverter.regression;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

import com.lytrax.accessconverter.Extraction;
import com.lytrax.accessconverter.fixtures.CorpusCase;
import com.lytrax.accessconverter.fixtures.CorpusFile;
import com.lytrax.accessconverter.fixtures.GeneratedFixture;
import com.lytrax.accessconverter.model.IndexModel;
import com.lytrax.accessconverter.model.IndexModel.IndexColumn;
import com.lytrax.accessconverter.model.IndexModel.Origin;
import com.lytrax.accessconverter.model.TableModel;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * F-30: Access's hidden relationship-backing indexes were exported as duplicate UNIQUE and plain indexes. F-37:
 * index direction was lost. The normalized model has neither problem.
 */
class IndexNormalizationTest {

    static Stream<CorpusCase> corpus() {
        return CorpusCase.databases();
    }

    @ParameterizedTest
    @MethodSource("corpus")
    void noHiddenBackingIndexSurvives(CorpusCase database) {
        for (TableModel table :
                Extraction.of(database.file(), database.options()).model().tables()) {
            for (IndexModel index : table.allIndexes()) {
                assertThat(index.name()).as(table.name()).doesNotStartWith(".r");
                // No two kept indexes have the same columns, directions and uniqueness
                assertThat(table.allIndexes())
                        .filteredOn(other -> other != index
                                && other.columns().equals(index.columns())
                                && other.unique() == index.unique()
                                && other.ignoreNulls() == index.ignoreNulls())
                        .as(table.name() + "." + index.name())
                        .isEmpty();
            }
        }
    }

    @Test
    void customersKeepsItsKeysOnly() {
        TableModel customers =
                Extraction.of(GeneratedFixture.SCHEMA_FIDELITY.path()).table("Customers");
        assertThat(customers.primaryKey().columnNames()).containsExactly("CustomerID");
        assertThat(customers.indexes())
                .extracting(IndexModel::name, IndexModel::unique, IndexModel::ignoreNulls)
                .containsExactly(
                        tuple("IX_Name", false, false), tuple("UX_Code", true, false), tuple("UX_Email", true, true));
    }

    @Test
    void childSideBackingIndexesMergeOrStay() {
        Extraction e = Extraction.of(GeneratedFixture.SCHEMA_FIDELITY.path());
        TableModel orders = e.table("Orders");
        assertThat(orders.indexes()).extracting(IndexModel::name).containsExactly("CustomerID", "ShippersOrders");
        assertThat(orders.indexes().getFirst().sourceNames()).containsExactly("CustomerID", "CustomersOrders");
        assertThat(orders.indexes().get(1).origin()).isEqualTo(Origin.RELATIONSHIP);

        TableModel details = e.table("Order Details");
        assertThat(details.primaryKey().sourceNames()).containsExactly("PrimaryKey", "OrdersOrder Details");
        assertThat(details.indexes()).extracting(IndexModel::name).containsExactly("ProductsOrder Details");
    }

    @Test
    void aPlainIndexOnThePrimaryKeyIsAbsorbed() {
        Extraction e =
                Extraction.of(CorpusFile.get("jackcess/V2010/indexV2010.accdb").file());
        for (String table : new String[] {"Table1", "Table2", "Table3"}) {
            assertThat(e.table(table).primaryKey().sourceNames()).containsExactly("PrimaryKey", "id");
            assertThat(e.table(table).indexes()).noneMatch(i -> i.name().equals("id"));
        }
    }

    @Test
    void directionIsKept() {
        TableModel table = Extraction.of(
                        CorpusFile.get("jackcess/V2019/extDateV2019.accdb").file())
                .table("Table1");
        assertThat(table.indexes())
                .extracting(IndexModel::name, IndexModel::columns)
                .containsExactly(
                        tuple("DateExtAsc", List.of(new IndexColumn("DateExt", true))),
                        tuple("DateExtDesc", List.of(new IndexColumn("DateExt", false))));
    }

    @Test
    void complexColumnsKeepTheirHiddenUniqueIndex() {
        // It becomes the UNIQUE key on the complex id in phase 5 (08)
        TableModel table = Extraction.of(
                        CorpusFile.get("jackcess/V2010/complexDataV2010.accdb").file())
                .table("Table1");
        assertThat(table.indexes())
                .filteredOn(i -> i.columnNames().equals(List.of("attach-data")))
                .singleElement()
                .satisfies(i -> assertThat(i.unique()).isTrue());
    }
}
