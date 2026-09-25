package io.lytrax.accessconverter.target.sqlite;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/** The value rules of 06: ISO-8601 text dates (F-05) and the exact-decimal decision (F-06). */
class SqliteValuesTest {

    @Test
    void datesAreIsoTextWithAFourDigitYear() {
        assertThat(SqliteValues.dateTime(LocalDateTime.of(2024, 1, 2, 3, 4, 5), 0))
                .isEqualTo("2024-01-02 03:04:05");
        // Access dates go back to year 100; a two-digit year would not sort or parse
        assertThat(SqliteValues.dateTime(LocalDateTime.of(201, 5, 5, 0, 0), 0)).isEqualTo("0201-05-05 00:00:00");
        // A time-only Access value is on day zero
        assertThat(SqliteValues.dateTime(SqliteValues.DAY_ZERO.withHour(10).withMinute(30), 0))
                .isEqualTo("1899-12-30 10:30:00");
    }

    @Test
    void fractionalSecondsFollowTheColumnsPrecision() {
        LocalDateTime value = LocalDateTime.of(2024, 1, 2, 3, 4, 5, 123_456_700);
        assertThat(SqliteValues.dateTime(value, 3)).isEqualTo("2024-01-02 03:04:05.123");
        assertThat(SqliteValues.dateTime(value, 7)).isEqualTo("2024-01-02 03:04:05.1234567");
        assertThat(SqliteValues.dateTime(value.withNano(0), 3)).isEqualTo("2024-01-02 03:04:05.000");
    }

    @Test
    void aValueThatDoesNotFitTheColumnsPrecisionIsReported() {
        LocalDateTime millis = LocalDateTime.of(2024, 1, 2, 3, 4, 5, 123_000_000);
        assertThat(SqliteValues.losesFraction(millis, 3)).isFalse();
        assertThat(SqliteValues.losesFraction(millis, 0)).isTrue();
        LocalDateTime hundredNanos = LocalDateTime.of(2024, 1, 2, 3, 4, 5, 123_456_700);
        assertThat(SqliteValues.losesFraction(hundredNanos, 7)).isFalse();
        assertThat(SqliteValues.losesFraction(hundredNanos, 3)).isTrue();
        assertThat(SqliteValues.losesFraction(LocalDateTime.of(2024, 1, 2, 3, 4, 5), 0))
                .isFalse();
    }

    @ParameterizedTest(name = "{0} needs {1} digits")
    @CsvSource({
        "0, 1, true",
        "12.3456, 6, true",
        "12.34560, 6, true",
        "-5.5, 2, true",
        "1234567890.123, 13, true",
        "0.1234567890123, 13, true",
        // Past 13 digits SQLite renders the REAL it stored with more digits than the value had (F-06)
        "1359351576355.8, 14, false",
        "19230.7692307692, 15, false",
        "123456789012345.1234, 19, false",
        "1234567890123456789012345678.123456789, 37, false"
    })
    void aNumericColumnOnlyKeepsThirteenSignificantDigits(String value, int digits, boolean fits) {
        assertThat(SqliteValues.significantDigits(new BigDecimal(value))).isEqualTo(digits);
        assertThat(SqliteValues.fitsNumeric(digits)).isEqualTo(fits);
    }
}
