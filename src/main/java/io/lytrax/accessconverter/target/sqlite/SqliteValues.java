package io.lytrax.accessconverter.target.sqlite;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

/**
 * How canonical values become the values SQLite stores (06, Column types). Nothing here depends on the default
 * locale or time zone, so the same database gives the same file everywhere.
 */
public final class SqliteValues {
    /**
     * Significant digits a NUMERIC column still holds exactly, in the sense that matters to a reader: SQLite
     * converts numeric text to a REAL (an IEEE double) and renders that REAL back with about 17 digits, so the
     * decimal only comes back unchanged while it is short enough for the double to carry its own text form.
     *
     * <p>Measured on SQLite 3.53.4 over random decimals: 13 digits came back unchanged 40 times out of 40, 14
     * digits failed 14 times and 15 digits 31 times ({@code 19230.7692307692} of {@code calcFieldV2010} reads back
     * as {@code 19230.769230769201}). Anything longer is kept as exact text instead, with
     * {@code DECIMAL_STORED_AS_TEXT} (F-06).
     */
    public static final int EXACT_DECIMAL_DIGITS = 13;

    /** Access's day zero: the date a time-only value belongs to. */
    public static final LocalDateTime DAY_ZERO = LocalDateTime.of(1899, 12, 30, 0, 0);

    /** Fractional-second digits for a date/time column whose values have any fraction at all. */
    public static final int DEFAULT_FRACTION_DIGITS = 3;

    /** Date/Time Extended holds 100 ns, which is 7 fractional digits. */
    public static final int EXTENDED_FRACTION_DIGITS = 7;

    private static final DateTimeFormatter[] FORMATS = new DateTimeFormatter[10];

    static {
        for (int digits = 0; digits < FORMATS.length; digits++) {
            String pattern = "uuuu-MM-dd HH:mm:ss" + (digits == 0 ? "" : "." + "S".repeat(digits));
            FORMATS[digits] = DateTimeFormatter.ofPattern(pattern, Locale.ROOT);
        }
    }

    private SqliteValues() {}

    /**
     * {@code 'YYYY-MM-DD HH:MM:SS'} with {@code digits} fractional digits: what SQLite's own date functions read,
     * and sortable as text. The year is always four digits, so Access's year 201 stays {@code 0201}.
     */
    public static String dateTime(LocalDateTime value, int digits) {
        return FORMATS[digits].format(value);
    }

    /** Whether writing {@code value} with {@code digits} fractional digits would drop part of its fraction. */
    public static boolean losesFraction(LocalDateTime value, int digits) {
        if (digits >= 9) {
            return false;
        }
        long unit = 1;
        for (int i = digits; i < 9; i++) {
            unit *= 10;
        }
        return value.getNano() % unit != 0;
    }

    /** Digits needed to hold the value exactly, ignoring trailing zeros: {@code 12.3400} needs 3. */
    public static int significantDigits(BigDecimal value) {
        return value.stripTrailingZeros().precision();
    }

    /** Whether a NUMERIC column stores every value of this size exactly. */
    public static boolean fitsNumeric(int significantDigits) {
        return significantDigits <= EXACT_DECIMAL_DIGITS;
    }
}
