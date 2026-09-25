package io.lytrax.accessconverter.verify;

import io.lytrax.accessconverter.model.AccessType;
import io.lytrax.accessconverter.value.ComplexRef;
import io.lytrax.accessconverter.value.OleValue;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;

/**
 * Compares a canonical source value with the value read back from a target, after the type-aware normalization of
 * 09 (The verifier, step 4). Targets hand back their own Java types: a SQLite REAL for a Single, a decimal string
 * for a Currency stored as TEXT, an ISO string for a date.
 *
 * <ul>
 *   <li>Exact numerics compare by value: {@code 12.3456} equals {@code 12.34560}, never through {@code double}.
 *   <li>Single compares as {@code float}; Double as {@code double}.
 *   <li>Dates compare at the target column's fractional-second precision, rounding half up as MySQL does.
 *   <li>Text and bytes compare exactly.
 * </ul>
 */
public final class ValueComparator {
    private ValueComparator() {}

    /** @param fractionDigits the target column's fractional-second digits (0–9); ignored for non-dates */
    public static boolean same(AccessType type, Object expected, Object actual, int fractionDigits) {
        if (expected == null || actual == null) {
            return expected == null && actual == null;
        }
        try {
            return switch (type) {
                case BOOLEAN -> (Boolean) expected == bool(actual);
                case BYTE, INT, LONG, AUTONUMBER_LONG, BIG_INT, MONEY, NUMERIC ->
                    decimal(expected).compareTo(decimal(actual)) == 0;
                case FLOAT ->
                    actual instanceof Number n
                            && Float.floatToIntBits((Float) expected) == Float.floatToIntBits(n.floatValue());
                case DOUBLE ->
                    actual instanceof Number n
                            && Double.doubleToLongBits((Double) expected) == Double.doubleToLongBits(n.doubleValue());
                case SHORT_DATE_TIME, EXT_DATE_TIME ->
                    round((LocalDateTime) expected, fractionDigits).equals(dateTime(actual));
                case TEXT, MEMO, HYPERLINK, GUID, AUTONUMBER_GUID -> expected.equals(actual);
                case BINARY, UNSUPPORTED -> Arrays.equals((byte[]) expected, bytes(actual));
                case OLE -> Arrays.equals(((OleValue) expected).raw(), bytes(actual));
                case ATTACHMENT, MULTI_VALUE, VERSION_HISTORY, COMPLEX_UNSUPPORTED ->
                    actual instanceof Number n && ((ComplexRef) expected).complexId() == n.longValue();
            };
        } catch (NumberFormatException | DateTimeParseException | ClassCastException e) {
            return false;
        }
    }

    /**
     * The value in a form where two values are {@link #same} exactly when their forms are equal: for comparing rows
     * whose order differs, by their digests. Null stays null.
     */
    public static String normalized(AccessType type, Object value, int fractionDigits) {
        if (value == null) {
            return null;
        }
        try {
            return switch (type) {
                case BOOLEAN -> value instanceof Boolean b ? (b ? "1" : "0") : bool(value) ? "1" : "0";
                case BYTE, INT, LONG, AUTONUMBER_LONG, BIG_INT, MONEY, NUMERIC ->
                    decimal(value).stripTrailingZeros().toPlainString();
                case FLOAT -> "f" + Integer.toHexString(Float.floatToIntBits(((Number) value).floatValue()));
                case DOUBLE -> "d" + Long.toHexString(Double.doubleToLongBits(((Number) value).doubleValue()));
                case SHORT_DATE_TIME, EXT_DATE_TIME ->
                    round(dateTime(value), fractionDigits).toString();
                case TEXT, MEMO, HYPERLINK, GUID, AUTONUMBER_GUID -> "s" + value;
                case BINARY, UNSUPPORTED, OLE -> "b" + java.util.HexFormat.of().formatHex(bytes(value));
                case ATTACHMENT, MULTI_VALUE, VERSION_HISTORY, COMPLEX_UNSUPPORTED ->
                    "c" + (value instanceof ComplexRef ref ? ref.complexId() : ((Number) value).longValue());
            };
        } catch (NumberFormatException | DateTimeParseException | ClassCastException e) {
            return "?" + value; // not comparable: equal to nothing a sound value normalizes to
        }
    }

    /** Rounds half up to {@code digits} fractional-second digits. */
    static LocalDateTime round(LocalDateTime t, int digits) {
        if (digits >= 9) {
            return t;
        }
        long unit = (long) Math.pow(10, 9 - digits);
        long nanos = t.getNano();
        long rounded = (nanos + unit / 2) / unit * unit;
        return t.truncatedTo(ChronoUnit.SECONDS).plusNanos(rounded);
    }

    private static boolean bool(Object actual) {
        return switch (actual) {
            case Boolean b -> b;
            case Number n -> n.longValue() != 0;
            default -> throw new ClassCastException("not a boolean: " + actual);
        };
    }

    private static BigDecimal decimal(Object value) {
        return switch (value) {
            case BigDecimal d -> d;
            case Double d -> new BigDecimal(d.toString());
            case Float f -> new BigDecimal(f.toString());
            case Number n -> new BigDecimal(n.toString());
            case String s -> new BigDecimal(s.strip());
            default -> throw new ClassCastException("not a number: " + value);
        };
    }

    private static LocalDateTime dateTime(Object actual) {
        return switch (actual) {
            case LocalDateTime t -> t;
            case String s -> LocalDateTime.parse(s.strip().replace(' ', 'T'));
            default -> throw new ClassCastException("not a date-time: " + actual);
        };
    }

    private static byte[] bytes(Object actual) {
        return switch (actual) {
            case byte[] b -> b;
            case OleValue ole -> ole.raw();
            default -> throw new ClassCastException("not bytes: " + actual);
        };
    }
}
