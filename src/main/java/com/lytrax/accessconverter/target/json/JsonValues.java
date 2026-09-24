package com.lytrax.accessconverter.target.json;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

/**
 * The value spellings of 07 that JSON has no type for: date-times, hyperlinks split into their parts, and text that
 * UTF-8 can't carry as it is.
 */
public final class JsonValues {

    /** Four-digit years, and {@code +10000} past 9999 (Money files use such dates as markers). */
    private static final DateTimeFormatter DATE = DateTimeFormatter.ISO_LOCAL_DATE.withLocale(Locale.ROOT);

    private static final DateTimeFormatter TIME = DateTimeFormatter.ofPattern("HH:mm:ss", Locale.ROOT);

    private JsonValues() {}

    /**
     * A Date/Time value: {@code YYYY-MM-DDTHH:MM:SS}, with {@code .fff} when it has milliseconds. Jackcess reads
     * Date/Time at millisecond resolution; a finer value (never seen) gets 6 or 9 digits rather than being rounded.
     */
    public static String dateTime(LocalDateTime value) {
        String text = DATE.format(value) + 'T' + TIME.format(value);
        int nanos = value.getNano();
        if (nanos == 0) {
            return text;
        }
        if (nanos % 1_000_000 == 0) {
            return text + String.format(Locale.ROOT, ".%03d", nanos / 1_000_000);
        }
        if (nanos % 1_000 == 0) {
            return text + String.format(Locale.ROOT, ".%06d", nanos / 1_000);
        }
        return text + String.format(Locale.ROOT, ".%09d", nanos);
    }

    /** A Date/Time Extended value: always 7 fraction digits, Access's 100-nanosecond resolution (F-10). */
    public static String extendedDateTime(LocalDateTime value) {
        int nanos = value.getNano();
        if (nanos % 100 != 0) {
            throw new IllegalStateException("a Date/Time Extended value finer than 100 ns: " + value);
        }
        return DATE.format(value) + 'T' + TIME.format(value) + String.format(Locale.ROOT, ".%07d", nanos / 100);
    }

    /**
     * An Access hyperlink, {@code display#address#subaddress#screentip}, split into its parts. A missing or empty part
     * is null; text without any {@code #} is display text only; a {@code #} inside the screen tip stays in it.
     */
    public record Hyperlink(String display, String address, String subAddress, String screenTip) {

        public static Hyperlink parse(String stored) {
            String[] parts = stored.split("#", 4);
            return new Hyperlink(part(parts, 0), part(parts, 1), part(parts, 2), screenTip(parts));
        }

        private static String part(String[] parts, int i) {
            return i < parts.length && !parts[i].isEmpty() ? parts[i] : null;
        }

        /** The last part keeps any further {@code #}, less the trailing one Access writes after the last part. */
        private static String screenTip(String[] parts) {
            if (parts.length < 4) {
                return null;
            }
            String tip = parts[3].endsWith("#") ? parts[3].substring(0, parts[3].length() - 1) : parts[3];
            return tip.isEmpty() ? null : tip;
        }
    }

    public static boolean hasUnpairedSurrogate(String text) {
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (Character.isHighSurrogate(c) && i + 1 < text.length() && Character.isLowSurrogate(text.charAt(i + 1))) {
                i++;
            } else if (Character.isSurrogate(c)) {
                return true;
            }
        }
        return false;
    }

    /**
     * A JSON string literal that keeps an unpaired surrogate as a {@code \\uXXXX} escape: JSON's grammar allows it and
     * it reads back as the same UTF-16 text, where UTF-8 has no bytes for it at all.
     */
    public static String quotedKeepingSurrogates(String text) {
        StringBuilder out = new StringBuilder(text.length() + 16).append('"');
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            boolean pair = Character.isHighSurrogate(c)
                    && i + 1 < text.length()
                    && Character.isLowSurrogate(text.charAt(i + 1));
            if (pair) {
                out.append(c).append(text.charAt(++i));
            } else if (c == '"' || c == '\\') {
                out.append('\\').append(c);
            } else if (c < 0x20 || Character.isSurrogate(c)) {
                out.append(String.format(Locale.ROOT, "\\u%04X", (int) c));
            } else {
                out.append(c);
            }
        }
        return out.append('"').toString();
    }
}
