package com.lytrax.accessconverter.target.mysql;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.HexFormat;
import java.util.Locale;

/**
 * How values and names are written in a MySQL/MariaDB dump (05, Value literals). Nothing here depends on the default
 * locale or time zone.
 *
 * <p>String literals rely on the dump's own {@code sql_mode}, which leaves out {@code NO_BACKSLASH_ESCAPES}: every
 * character the server or the {@code mysql} client would read as special is escaped with a backslash (F-02).
 */
public final class MySqlLiterals {

    /** The latest value a {@code DATETIME} holds. */
    private static final LocalDateTime MAX_DATETIME = LocalDateTime.of(9999, 12, 31, 23, 59, 59, 999_999_000);

    private static final DateTimeFormatter[] FORMATS = new DateTimeFormatter[7];

    private static final HexFormat HEX = HexFormat.of().withUpperCase();

    /** U+FFFD, written for what UTF-8 can't encode. */
    static final char REPLACEMENT = (char) 0xFFFD;

    static {
        for (int digits = 0; digits < FORMATS.length; digits++) {
            String pattern = "uuuu-MM-dd HH:mm:ss" + (digits == 0 ? "" : "." + "S".repeat(digits));
            FORMATS[digits] = DateTimeFormatter.ofPattern(pattern, Locale.ROOT);
        }
    }

    private MySqlLiterals() {}

    /** A quoted identifier: {@code `name`}, with {@code `} doubled. */
    public static String identifier(String name) {
        return "`" + name.replace("`", "``") + "`";
    }

    /** A string literal in single quotes. */
    public static String string(String text) {
        StringBuilder out = new StringBuilder(text.length() + 2);
        string(out, text);
        return out.toString();
    }

    /**
     * Appends a string literal: {@code \0 \' \" \b \n \r \t \Z \\} are escaped, everything else is written as it
     * is. Unpaired surrogates must have been replaced by the caller: they have no UTF-8 form.
     */
    public static void string(StringBuilder out, String text) {
        out.append('\'');
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            switch (c) {
                case '\0' -> out.append("\\0");
                case '\'' -> out.append("\\'");
                case '"' -> out.append("\\\"");
                case '\b' -> out.append("\\b");
                case '\n' -> out.append("\\n");
                case '\r' -> out.append("\\r");
                case '\t' -> out.append("\\t");
                case '\u001A' -> out.append("\\Z");
                case '\\' -> out.append("\\\\");
                default -> out.append(c);
            }
        }
        out.append('\'');
    }

    /** Whether a string holds a lone surrogate, which no UTF-8 text (and so no utf8mb4 column) can hold. */
    public static boolean hasUnpairedSurrogate(String text) {
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (Character.isHighSurrogate(c)) {
                if (i + 1 < text.length() && Character.isLowSurrogate(text.charAt(i + 1))) {
                    i++;
                    continue;
                }
                return true;
            }
            if (Character.isLowSurrogate(c)) {
                return true;
            }
        }
        return false;
    }

    /** The text with every lone surrogate replaced by U+FFFD. */
    public static String replaceUnpairedSurrogates(String text) {
        StringBuilder out = new StringBuilder(text.length());
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (Character.isHighSurrogate(c) && i + 1 < text.length() && Character.isLowSurrogate(text.charAt(i + 1))) {
                out.append(c).append(text.charAt(++i));
            } else if (Character.isSurrogate(c)) {
                out.append(REPLACEMENT);
            } else {
                out.append(c);
            }
        }
        return out.toString();
    }

    /** An exact decimal: all its digits, never an exponent. */
    public static String decimal(BigDecimal value) {
        return value.toPlainString();
    }

    /**
     * A Single for a {@code FLOAT} column. {@code Float.toString} is the shortest text that identifies the value, but
     * the server parses it as a double and range-checks that before rounding to a float: {@code 3.4028235E38} (the
     * largest Single) is over the float range as a double and fails with ERROR 1264 (measured on all five servers).
     * Such a value is written as the exact double of the float instead, which the server stores unchanged.
     */
    public static String floatValue(float value) {
        String shortest = Float.toString(value);
        double parsed = Double.parseDouble(shortest);
        if ((float) parsed == value && Math.abs(parsed) <= Float.MAX_VALUE) {
            return shortest;
        }
        return Double.toString(value);
    }

    /** A Double: the shortest text that identifies it; the server accepts {@code 1.0E10}. */
    public static String doubleValue(double value) {
        return Double.toString(value);
    }

    /**
     * A date/time rounded half up to {@code digits} fractional-second digits (0–6), as the server itself rounds, so
     * the value written is the value stored. A value that would round past the last {@code DATETIME} is cut instead.
     */
    public static LocalDateTime atPrecision(LocalDateTime value, int digits) {
        long unit = 1;
        for (int i = digits; i < 9; i++) {
            unit *= 10;
        }
        long nanos = value.getNano();
        long rounded = (nanos + unit / 2) / unit * unit;
        LocalDateTime result = value.truncatedTo(ChronoUnit.SECONDS).plusNanos(rounded);
        if (result.isAfter(MAX_DATETIME)) {
            return value.truncatedTo(ChronoUnit.SECONDS).plusNanos(nanos / unit * unit);
        }
        return result;
    }

    /** Whether a {@code DATETIME} holds the value: years 0000 to 9999 (measured in strict mode on both dialects). */
    public static boolean fitsDateTime(LocalDateTime value) {
        return value.getYear() >= 0 && value.getYear() <= 9999;
    }

    /** Whether writing {@code value} with {@code digits} fractional digits changes it. */
    public static boolean losesFraction(LocalDateTime value, int digits) {
        return !atPrecision(value, digits).equals(value);
    }

    /** {@code 'YYYY-MM-DD HH:MM:SS[.ffffff]'} with exactly {@code digits} fractional digits; not rounded here. */
    public static String dateTime(LocalDateTime value, int digits) {
        return "'" + dateTimeText(value, digits) + "'";
    }

    /** As {@link #dateTime}, without the quotes. The year always has four digits, so year 201 is {@code 0201}. */
    public static String dateTimeText(LocalDateTime value, int digits) {
        return FORMATS[digits].format(value);
    }

    /** {@code X'…'}: exact bytes, {@code X''} for an empty value (never NULL). */
    public static String bytes(byte[] value) {
        return "X'" + HEX.formatHex(value) + "'";
    }

    /** Appends {@code X'…'}. */
    public static void bytes(StringBuilder out, byte[] value) {
        out.append("X'");
        HEX.formatHex(out, value);
        out.append('\'');
    }

    /** The length of the text encoded as UTF-8, without encoding it. */
    public static long utf8Length(CharSequence text) {
        long length = 0;
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (c < 0x80) {
                length++;
            } else if (c < 0x800) {
                length += 2;
            } else if (Character.isHighSurrogate(c)
                    && i + 1 < text.length()
                    && Character.isLowSurrogate(text.charAt(i + 1))) {
                length += 4;
                i++;
            } else {
                length += 3;
            }
        }
        return length;
    }
}
