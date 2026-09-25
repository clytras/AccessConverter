package io.lytrax.accessconverter.verify;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

import io.lytrax.accessconverter.Extraction;
import io.lytrax.accessconverter.fixtures.GeneratedFixture;
import io.lytrax.accessconverter.model.TableModel;
import io.lytrax.accessconverter.source.AccessSource;
import io.lytrax.accessconverter.source.RowStream;
import io.lytrax.accessconverter.value.ComplexRef;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

/** The source side of verify: stable digests, and a row comparison that finds every difference. */
class SourceSnapshotTest {

    @Test
    void digestsAreStable() throws IOException {
        Extraction extraction = Extraction.of(GeneratedFixture.SCHEMA_FIDELITY.path());
        SourceSnapshot first;
        SourceSnapshot second;
        try (AccessSource source = AccessSource.open(GeneratedFixture.SCHEMA_FIDELITY.path())) {
            first = SourceSnapshot.capture(source, extraction.model());
            second = SourceSnapshot.capture(source, extraction.model());
        }
        assertThat(first).isEqualTo(second);
        assertThat(first.tables())
                .filteredOn(t -> t.table().equals("Customers"))
                .singleElement()
                .satisfies(t -> assertThat(t.rows()).isEqualTo(3));
    }

    @Test
    void theEncodingIdentifiesValuesNotJavaTypes() {
        assertThat(SourceSnapshot.encode((short) 5)).isEqualTo(SourceSnapshot.encode(5L));
        assertThat(SourceSnapshot.encode(new BigDecimal("12.3450"))).isEqualTo("D12.345");
        assertThat(SourceSnapshot.encode("")).isNotEqualTo(SourceSnapshot.encode(null));
        assertThat(SourceSnapshot.encode("a:b")).isEqualTo("S3:a:b");
        assertThat(SourceSnapshot.encode(new ComplexRef(3))).isEqualTo("C3");
    }

    @Test
    void aTableComparedWithItselfMatches() throws IOException {
        Extraction extraction = Extraction.of(GeneratedFixture.SCHEMA_FIDELITY.path());
        TableModel customers = extraction.table("Customers");
        try (AccessSource source = AccessSource.open(GeneratedFixture.SCHEMA_FIDELITY.path())) {
            TableComparison.Result result = TableComparison.compare(
                    customers, source.rows(customers), source.rows(customers), precision(customers));
            assertThat(result.matches()).isTrue();
            assertThat(result.expectedRows()).isEqualTo(3);
        }
    }

    @Test
    void everyDifferenceIsFound() throws IOException {
        Extraction extraction = Extraction.of(GeneratedFixture.SCHEMA_FIDELITY.path());
        TableModel customers = extraction.table("Customers");
        List<Object[]> altered = new ArrayList<>();
        try (AccessSource source = AccessSource.open(GeneratedFixture.SCHEMA_FIDELITY.path())) {
            RowStream rows = source.rows(customers);
            rows.forEachRemaining(r -> altered.add(r.clone()));
            int rating = customers.columns().indexOf(customers.column("Rating").orElseThrow());
            altered.get(0)[rating] = -56; // what v2 wrote for 200 (F-01)
            altered.remove(2);

            TableComparison.Result result = TableComparison.compare(
                    customers, source.rows(customers), altered.iterator(), precision(customers));
            assertThat(result.differenceCount()).isEqualTo(2);
            assertThat(result.differences())
                    .extracting(TableComparison.Difference::kind, TableComparison.Difference::column)
                    .containsExactly(
                            tuple(TableComparison.Kind.VALUE, "Rating"), tuple(TableComparison.Kind.MISSING_ROW, null));
            assertThat(result.differences().getFirst().rowKey()).isEqualTo("(-5)");
        }
    }

    private static int[] precision(TableModel table) {
        int[] digits = new int[table.columns().size()];
        Arrays.fill(digits, 9);
        return digits;
    }
}
