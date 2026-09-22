package com.lytrax.accessconverter.value;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HexFormat;
import java.util.Locale;

/**
 * Locale-independent text for canonical values, for {@code inspect}, report samples and digests. The same value
 * gives the same text on every machine: no default locale (the build runs under el-GR, which has a decimal comma)
 * and no time zone.
 */
public final class CanonicalText {
    private static final DateTimeFormatter SECONDS = DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ss", Locale.ROOT);
    private static final int MAX_BYTES_SHOWN = 16;

    private CanonicalText() {}

    /** A short, readable form: strings quoted, byte arrays as leading hex plus size. */
    public static String of(Object value) {
        return switch (value) {
            case null -> "NULL";
            case String s -> "\"" + s.replace("\"", "\"\"") + "\"";
            case BigDecimal d -> d.toPlainString();
            case LocalDateTime t -> dateTime(t);
            case byte[] b -> bytes(b);
            case OleValue ole -> "OLE " + bytes(ole.raw());
            case ComplexRef ref -> "complex#" + ref.complexId();
            default -> value.toString();
        };
    }

    /** ISO-8601 local date-time; the fraction appears only when non-zero, with trailing zeros removed. */
    public static String dateTime(LocalDateTime t) {
        String text = SECONDS.format(t);
        int nanos = t.getNano();
        if (nanos == 0) {
            return text;
        }
        String fraction = String.format(Locale.ROOT, "%09d", nanos).replaceFirst("0+$", "");
        return text + "." + fraction;
    }

    public static String sha256(byte[] bytes) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("every JDK provides SHA-256", e);
        }
    }

    public static String sha256(String text) {
        return sha256(text.getBytes(StandardCharsets.UTF_8));
    }

    private static String bytes(byte[] b) {
        String hex = HexFormat.of().formatHex(b, 0, Math.min(b.length, MAX_BYTES_SHOWN));
        return "0x" + hex + (b.length > MAX_BYTES_SHOWN ? "…" : "") + " (" + b.length + " bytes)";
    }
}
