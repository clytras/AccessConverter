package com.lytrax.accessconverter.extract;

import static org.assertj.core.api.Assertions.assertThat;

import com.lytrax.accessconverter.extract.IndexNormalizer.RawIndex;
import com.lytrax.accessconverter.extract.IndexNormalizer.Result;
import com.lytrax.accessconverter.model.IndexModel;
import com.lytrax.accessconverter.model.IndexModel.IndexColumn;
import com.lytrax.accessconverter.model.IndexModel.Origin;
import com.lytrax.accessconverter.report.Issue;
import com.lytrax.accessconverter.report.IssueCode;
import com.lytrax.accessconverter.report.Issues;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

/** The rules of 04 (Index normalization) on synthetic logical indexes; the corpus tests are in regression/. */
class IndexNormalizerTest {
    private final Issues issues = new Issues();

    @Test
    void theReferencedSideBackingIndexRepeatsThePrimaryKey() {
        Result r = normalize(pk("PrimaryKey", "CustomerID"), fk(".rB", true, "CustomerID"));
        assertThat(r.primaryKey().name()).isEqualTo("PrimaryKey");
        assertThat(r.indexes()).isEmpty();
        assertThat(codes()).containsExactly(IssueCode.INDEX_BACKING_DROPPED);
    }

    @Test
    void theChildSideBackingIndexIsKeptUnderTheRelationshipsName() {
        Result r = normalize(pk("PrimaryKey", "OrderID"), fk("ShippersOrders", false, "ShipperCode"));
        assertThat(r.indexes()).singleElement().satisfies(i -> {
            assertThat(i.name()).isEqualTo("ShippersOrders");
            assertThat(i.origin()).isEqualTo(Origin.RELATIONSHIP);
            assertThat(i.unique()).isFalse();
        });
        assertThat(codes()).isEmpty();
    }

    @Test
    void theChildSideBackingIndexMergesIntoAnIndexThatStartsWithItsColumns() {
        Result r = normalize(
                pk("PrimaryKey", "OrderID", "ProductID"),
                plain("CustomerID", "CustomerID"),
                fk("CustomersOrders", false, "CustomerID"),
                fk("OrdersOrder Details", false, "OrderID"));
        assertThat(r.primaryKey().sourceNames()).containsExactly("PrimaryKey", "OrdersOrder Details");
        assertThat(r.indexes()).singleElement().satisfies(i -> {
            assertThat(i.name()).isEqualTo("CustomerID");
            assertThat(i.origin()).isEqualTo(Origin.USER);
            assertThat(i.sourceNames()).containsExactly("CustomerID", "CustomersOrders");
        });
        assertThat(codes()).containsExactly(IssueCode.INDEX_MERGED_DUPLICATE, IssueCode.INDEX_MERGED_DUPLICATE);
    }

    @Test
    void aPlainIndexOnThePrimaryKeyColumnsIsAbsorbed() {
        // indexV2010: a plain `id` index and the unique PrimaryKey, both on id (maintainer decision, phase 1)
        Result r = normalize(plain("id", "id"), pk("PrimaryKey", "id"));
        assertThat(r.primaryKey().sourceNames()).containsExactly("PrimaryKey", "id");
        assertThat(r.indexes()).isEmpty();
    }

    @Test
    void aPlainIndexIsAbsorbedByAUniqueOneWhateverTheirNames() {
        Result r = normalize(plain("A_plain", "Code"), unique("Z_unique", "Code"));
        assertThat(r.indexes()).singleElement().satisfies(i -> {
            assertThat(i.name()).isEqualTo("Z_unique");
            assertThat(i.sourceNames()).containsExactly("Z_unique", "A_plain");
        });
    }

    @Test
    void directionAndPrefixesMakeIndexesDifferent() {
        Result r = normalize(
                plain("DateAsc", "Date"),
                raw("DateDesc", false, false, false, false, false, desc("Date")),
                plain("DateId", "Date", "ID"));
        assertThat(r.indexes()).extracting(IndexModel::name).containsExactly("DateAsc", "DateDesc", "DateId");
        assertThat(codes()).isEmpty();
    }

    @Test
    void uniqueIndexesMergeOnlyOnTheSameSignature() {
        Result r = normalize(
                unique("UX_A", "Email"),
                unique("UX_B", "Email"),
                raw("UX_C", false, false, true, false, true, asc("Email")));
        assertThat(r.indexes()).extracting(IndexModel::name).containsExactly("UX_A", "UX_C");
        assertThat(r.indexes().getFirst().sourceNames()).containsExactly("UX_A", "UX_B");
    }

    @Test
    void aUniqueBackingIndexWithoutAMatchingKeyIsKept() {
        // One-to-one child side on columns that aren't otherwise unique: Access enforces it
        Result r = normalize(pk("PrimaryKey", "ID"), fk("PeoplePassports", true, "PersonID"));
        assertThat(r.indexes()).singleElement().satisfies(i -> {
            assertThat(i.unique()).isTrue();
            assertThat(i.origin()).isEqualTo(Origin.RELATIONSHIP);
        });
    }

    @Test
    void uniquenessOfASetIgnoresColumnOrder() {
        Result r = normalize(pk("PrimaryKey", "A", "B"), fk(".rB", true, "B", "A"));
        assertThat(r.indexes()).isEmpty();
        assertThat(codes()).containsExactly(IssueCode.INDEX_BACKING_DROPPED);
    }

    @Test
    void duplicateBackingIndexNamesAreEachAccountedFor() {
        normalize(pk("PrimaryKey", "id"), fk(".rC", true, "id"), fk(".rC", true, "id"));
        assertThat(issues.list())
                .singleElement()
                .satisfies(i -> assertThat(i.count()).isEqualTo(2));
    }

    @Test
    void requiredIsKeptWhenMerging() {
        Result r = normalize(
                raw("IX1", false, false, false, false, false, asc("X")),
                raw("IX2", false, false, false, true, false, asc("X")));
        assertThat(r.indexes())
                .singleElement()
                .satisfies(i -> assertThat(i.required()).isTrue());
    }

    @Test
    void withoutAPrimaryKey() {
        Result r = normalize(plain("IX", "X"));
        assertThat(r.primaryKey()).isNull();
        assertThat(r.indexes()).hasSize(1);
    }

    private Result normalize(RawIndex... raw) {
        return IndexNormalizer.normalize("T", List.of(raw), issues);
    }

    private List<IssueCode> codes() {
        return issues.list().stream().map(Issue::code).toList();
    }

    private static RawIndex pk(String name, String... columns) {
        return raw(name, true, false, true, true, false, asc(columns));
    }

    private static RawIndex plain(String name, String... columns) {
        return raw(name, false, false, false, false, false, asc(columns));
    }

    private static RawIndex unique(String name, String... columns) {
        return raw(name, false, false, true, false, false, asc(columns));
    }

    private static RawIndex fk(String name, boolean unique, String... columns) {
        return raw(name, false, true, unique, unique, false, asc(columns));
    }

    private static RawIndex raw(
            String name,
            boolean primaryKey,
            boolean foreignKey,
            boolean unique,
            boolean required,
            boolean ignoreNulls,
            List<IndexColumn> columns) {
        return new RawIndex(name, columns, primaryKey, foreignKey, unique, ignoreNulls, required);
    }

    private static List<IndexColumn> asc(String... names) {
        return Arrays.stream(names).map(n -> new IndexColumn(n, true)).toList();
    }

    private static List<IndexColumn> desc(String... names) {
        return Arrays.stream(names).map(n -> new IndexColumn(n, false)).toList();
    }
}
