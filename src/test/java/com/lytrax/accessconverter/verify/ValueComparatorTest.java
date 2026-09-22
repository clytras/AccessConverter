package com.lytrax.accessconverter.verify;

import static org.assertj.core.api.Assertions.assertThat;

import com.lytrax.accessconverter.model.AccessType;
import com.lytrax.accessconverter.value.OleValue;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import org.junit.jupiter.api.Test;

/** 09, step 4: values from a target, in the target's own Java types, against the canonical source value. */
class ValueComparatorTest {

    @Test
    void exactNumericsCompareByValue() {
        BigDecimal money = new BigDecimal("123456789012345.1234");
        assertThat(same(AccessType.MONEY, money, "123456789012345.1234")).isTrue();
        assertThat(same(AccessType.MONEY, money, new BigDecimal("123456789012345.12340")))
                .isTrue();
        // What SQLite's NUMERIC affinity does to it (F-06)
        assertThat(same(AccessType.MONEY, money, 1.2345678901234512E14)).isFalse();
        assertThat(same(AccessType.NUMERIC, new BigDecimal("12.3456"), 12.3456d))
                .isTrue();
    }

    @Test
    void integersAcceptAnyIntegralRepresentation() {
        assertThat(same(AccessType.BYTE, 200, 200L)).isTrue();
        assertThat(same(AccessType.BYTE, 200, -56)).isFalse(); // F-01
        assertThat(same(AccessType.AUTONUMBER_LONG, -5, (short) -5)).isTrue();
        assertThat(same(AccessType.BOOLEAN, true, 1)).isTrue();
        assertThat(same(AccessType.BOOLEAN, false, 0L)).isTrue();
    }

    @Test
    void singlesCompareAsFloat() {
        // SQLite stores Double.parseDouble(Float.toString(v)) (06)
        assertThat(same(AccessType.FLOAT, 2583.2092f, 2583.2092d)).isTrue();
        assertThat(same(AccessType.FLOAT, 2583.2092f, 2583.21d)).isFalse(); // F-07
        assertThat(same(AccessType.DOUBLE, 0.1d, 0.1d)).isTrue();
    }

    @Test
    void datesCompareAtTheTargetPrecision() {
        LocalDateTime ext = LocalDateTime.of(2024, 1, 2, 3, 4, 5, 123_456_700);
        assertThat(ValueComparator.same(AccessType.EXT_DATE_TIME, ext, "2024-01-02 03:04:05.1234567", 7))
                .isTrue();
        assertThat(ValueComparator.same(
                        AccessType.EXT_DATE_TIME, ext, LocalDateTime.of(2024, 1, 2, 3, 4, 5, 123_457_000), 6))
                .isTrue();
        assertThat(ValueComparator.same(AccessType.SHORT_DATE_TIME, ext, "2024-01-02T03:04:05", 0))
                .isTrue();
        assertThat(ValueComparator.round(LocalDateTime.of(2024, 12, 31, 23, 59, 59, 999_999_999), 0))
                .isEqualTo(LocalDateTime.of(2025, 1, 1, 0, 0));
    }

    @Test
    void textAndBytesCompareExactly() {
        assertThat(same(AccessType.TEXT, "C:\\temp\\new", "C:\\temp\\new")).isTrue();
        assertThat(same(AccessType.TEXT, "abc", "ABC")).isFalse();
        assertThat(same(AccessType.BINARY, new byte[0], new byte[0])).isTrue();
        assertThat(same(AccessType.OLE, new OleValue(new byte[] {1, 2}), new byte[] {1, 2}))
                .isTrue();
    }

    @Test
    void nullOnlyEqualsNull() {
        assertThat(same(AccessType.DOUBLE, null, null)).isTrue();
        assertThat(same(AccessType.DOUBLE, 0d, null)).isFalse(); // F-04
        assertThat(same(AccessType.DOUBLE, null, 0d)).isFalse();
    }

    @Test
    void unreadableValuesAreDifferencesNotCrashes() {
        assertThat(same(AccessType.MONEY, BigDecimal.ONE, "one")).isFalse();
        assertThat(same(AccessType.SHORT_DATE_TIME, LocalDateTime.of(2024, 1, 1, 0, 0), "yesterday"))
                .isFalse();
    }

    private static boolean same(AccessType type, Object expected, Object actual) {
        return ValueComparator.same(type, expected, actual, 9);
    }
}
