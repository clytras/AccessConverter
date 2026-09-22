package com.lytrax.accessconverter.value;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import org.junit.jupiter.api.Test;

/** Runs under el-GR (decimal comma) and en-US in the surefire matrix: the text must be the same in both. */
class CanonicalTextTest {

    @Test
    void numbersIgnoreTheDefaultLocale() {
        assertThat(CanonicalText.of(new BigDecimal("1234.5"))).isEqualTo("1234.5");
        assertThat(CanonicalText.of(new BigDecimal("1E+3"))).isEqualTo("1000");
        assertThat(CanonicalText.of(0.1f)).isEqualTo("0.1");
        assertThat(CanonicalText.of(-2.5d)).isEqualTo("-2.5");
    }

    @Test
    void datesAreIsoWithTheFractionTheyHave() {
        assertThat(CanonicalText.dateTime(LocalDateTime.of(2024, 1, 2, 3, 4, 5)))
                .isEqualTo("2024-01-02T03:04:05");
        assertThat(CanonicalText.dateTime(LocalDateTime.of(2024, 1, 2, 3, 4, 5, 120_000_000)))
                .isEqualTo("2024-01-02T03:04:05.12");
        assertThat(CanonicalText.dateTime(LocalDateTime.of(201, 5, 5, 0, 0, 0, 100)))
                .isEqualTo("0201-05-05T00:00:00.0000001");
    }

    @Test
    void valuesAreRecognizable() {
        assertThat(CanonicalText.of(null)).isEqualTo("NULL");
        assertThat(CanonicalText.of("say \"hi\"")).isEqualTo("\"say \"\"hi\"\"\"");
        assertThat(CanonicalText.of(new byte[] {(byte) 0xCA, (byte) 0xFE})).isEqualTo("0xcafe (2 bytes)");
        assertThat(CanonicalText.of(new ComplexRef(7))).isEqualTo("complex#7");
    }
}
