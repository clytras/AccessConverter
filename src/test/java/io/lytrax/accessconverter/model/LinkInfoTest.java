package io.lytrax.accessconverter.model;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/** A linked table's ODBC password never reaches a message or inspect's output. */
class LinkInfoTest {

    @Test
    void anOdbcPasswordIsMaskedForDisplay() {
        TableModel.LinkInfo link =
                new TableModel.LinkInfo("ODBC;DSN=Shop;UID=sa;PWD=s3cret;DATABASE=shop", "dbo.Orders", true);
        assertThat(link.displayDatabase()).isEqualTo("ODBC;DSN=Shop;UID=sa;PWD=***;DATABASE=shop");
        assertThat(new TableModel.LinkInfo("DSN=x;Password=a b c", "t", true).displayDatabase())
                .isEqualTo("DSN=x;Password=***");
        assertThat(new TableModel.LinkInfo("C:\\data\\backend.accdb", "Orders", false).displayDatabase())
                .isEqualTo("C:\\data\\backend.accdb");
        assertThat(link.database()).contains("s3cret");
    }
}
