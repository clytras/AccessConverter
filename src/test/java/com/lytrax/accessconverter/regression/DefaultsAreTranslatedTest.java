package com.lytrax.accessconverter.regression;

import static org.assertj.core.api.Assertions.assertThat;

import com.lytrax.accessconverter.Extraction;
import com.lytrax.accessconverter.fixtures.CorpusCase;
import com.lytrax.accessconverter.fixtures.GeneratedFixture;
import com.lytrax.accessconverter.fixtures.LocalSample;
import com.lytrax.accessconverter.fixtures.LocalSamples;
import com.lytrax.accessconverter.model.ColumnModel;
import com.lytrax.accessconverter.model.DefaultValue;
import com.lytrax.accessconverter.model.expr.Expr;
import com.lytrax.accessconverter.model.expr.ExprPrinter;
import java.math.BigDecimal;
import java.util.stream.Stream;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * F-09: Access defaults were copied verbatim ({@code '=0'}, {@code Now()}, {@code Yes}). F-08: columns without
 * one got an invented {@code DEFAULT 0} or {@code ''}. In the model, a default is Access's own, parsed, and
 * nothing else.
 */
class DefaultsAreTranslatedTest {

    @Test
    void schemaFidelityDefaults() {
        Extraction e = Extraction.of(GeneratedFixture.SCHEMA_FIDELITY.path());
        assertThat(e.column("Customers", "Created").defaultValue().expr())
                .isEqualTo(new Expr.CurrentDateTime(Expr.CurrentDateTime.Part.NOW));
        assertThat(e.column("Customers", "Active").defaultValue().expr()).isEqualTo(new Expr.BooleanLiteral(true));
        assertThat(e.column("Customers", "Country").defaultValue().expr()).isEqualTo(new Expr.StringLiteral("Greece"));
    }

    @Test
    void columnsWithoutAnAccessDefaultHaveNone() {
        Extraction e = Extraction.of(GeneratedFixture.SCHEMA_FIDELITY.path());
        for (String column : new String[] {"Code", "Email", "Notes", "Rating", "Balance", "RowGuid", "Visits"}) {
            assertThat(e.column("Customers", column).defaultValue()).as(column).isNull();
        }
    }

    static Stream<CorpusCase> corpus() {
        return CorpusCase.databases();
    }

    /** Every default in the corpus is inside the supported subset; a regression would show as unsupported. */
    @ParameterizedTest
    @MethodSource("corpus")
    void everyCorpusDefaultTranslates(CorpusCase database) {
        Extraction.of(database.file(), database.options()).model().tables().stream()
                .flatMap(t -> t.columns().stream())
                .map(ColumnModel::defaultValue)
                .filter(d -> d != null)
                .forEach(d -> assertThat(d.unsupportedReason()).as(d.raw()).isNull());
    }

    @Nested
    @LocalSamples
    class Samples {
        @Test
        void marketBasketEqualsZero() {
            // v2 wrote DEFAULT '=0' on an integer: ERROR 1067, and table StoreList was lost
            Extraction e = Extraction.of(LocalSample.MARKET_BASKET.path());
            DefaultValue equalsZero = e.model().tables().stream()
                    .flatMap(t -> t.columns().stream())
                    .map(ColumnModel::defaultValue)
                    .filter(d -> d != null && d.raw().equals("=0"))
                    .findFirst()
                    .orElseThrow();
            assertThat(equalsZero.expr()).isEqualTo(new Expr.NumberLiteral(BigDecimal.ZERO));
            assertThat(ExprPrinter.print(equalsZero.expr())).isEqualTo("0");
        }
    }
}
