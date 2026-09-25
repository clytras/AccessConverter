package io.lytrax.accessconverter.regression;

import static org.assertj.core.api.Assertions.assertThat;

import io.lytrax.accessconverter.Extraction;
import io.lytrax.accessconverter.fixtures.GeneratedFixture;
import io.lytrax.accessconverter.model.AccessType;
import io.lytrax.accessconverter.model.TableModel;
import io.lytrax.accessconverter.source.AccessSource;
import io.lytrax.accessconverter.source.RowStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/** F-01: Access's Byte is unsigned; Jackcess returns it signed, so v2 wrote 200 as -56 and 255 as -1. */
class ByteIsUnsignedTest {

    @Test
    void ratingsReadAsZeroToTwoFiftyFive() throws IOException {
        Extraction extraction = Extraction.of(GeneratedFixture.SCHEMA_FIDELITY.path());
        TableModel customers = extraction.table("Customers");
        assertThat(extraction.column("Customers", "Rating").type()).isEqualTo(AccessType.BYTE);

        int rating = customers.columns().indexOf(customers.column("Rating").orElseThrow());
        List<Object> ratings = new ArrayList<>();
        try (AccessSource source = AccessSource.open(GeneratedFixture.SCHEMA_FIDELITY.path())) {
            RowStream rows = source.rows(customers);
            rows.forEachRemaining(row -> ratings.add(row[rating]));
        }
        // PK order: CustomerID -5, 1, 2
        assertThat(ratings).containsExactly(200, 255, 0);
    }
}
