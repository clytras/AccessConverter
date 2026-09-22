package com.lytrax.accessconverter.value;

import com.lytrax.accessconverter.model.AccessType;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Locale;

/**
 * The single place where a value as Jackcess returns it becomes its canonical Java type (03, Canonical values).
 * NULL stays null, nothing is substituted, and an unexpected Java type is a bug that fails loudly.
 *
 * <table>
 *   <tr><th>Access</th><th>Canonical</th></tr>
 *   <tr><td>BOOLEAN</td><td>Boolean</td></tr>
 *   <tr><td>BYTE</td><td>Integer 0–255 (Jackcess returns a signed Byte: F-01)</td></tr>
 *   <tr><td>INT</td><td>Short</td></tr>
 *   <tr><td>LONG, AUTONUMBER_LONG</td><td>Integer</td></tr>
 *   <tr><td>BIG_INT</td><td>Long</td></tr>
 *   <tr><td>MONEY, NUMERIC</td><td>BigDecimal, scale kept</td></tr>
 *   <tr><td>FLOAT / DOUBLE</td><td>Float / Double</td></tr>
 *   <tr><td>SHORT_DATE_TIME, EXT_DATE_TIME</td><td>LocalDateTime, no time zone</td></tr>
 *   <tr><td>TEXT, MEMO, HYPERLINK</td><td>String</td></tr>
 *   <tr><td>GUID, AUTONUMBER_GUID</td><td>String {@code {XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX}}, uppercase</td></tr>
 *   <tr><td>BINARY, UNSUPPORTED</td><td>byte[], empty stays empty</td></tr>
 *   <tr><td>OLE</td><td>{@link OleValue}</td></tr>
 *   <tr><td>complex types</td><td>{@link ComplexRef}</td></tr>
 * </table>
 */
public final class AccessValues {
    private AccessValues() {}

    public static Object canonical(AccessType type, Object raw) {
        if (raw == null) {
            return null;
        }
        return switch (type) {
            case BOOLEAN -> expect(type, raw, Boolean.class);
            case BYTE -> Byte.toUnsignedInt(expect(type, raw, Byte.class));
            case INT -> expect(type, raw, Short.class);
            case LONG, AUTONUMBER_LONG -> expect(type, raw, Integer.class);
            case BIG_INT -> expect(type, raw, Long.class);
            case MONEY, NUMERIC -> expect(type, raw, BigDecimal.class);
            case FLOAT -> expect(type, raw, Float.class);
            case DOUBLE -> expect(type, raw, Double.class);
            case SHORT_DATE_TIME, EXT_DATE_TIME -> expect(type, raw, LocalDateTime.class);
            case TEXT, MEMO, HYPERLINK -> expect(type, raw, String.class);
            case GUID, AUTONUMBER_GUID -> guid(expect(type, raw, String.class));
            case BINARY, UNSUPPORTED -> expect(type, raw, byte[].class);
            case OLE -> new OleValue(expect(type, raw, byte[].class));
            // Jackcess's ComplexValueForeignKey is a Number holding the complex id
            case ATTACHMENT, MULTI_VALUE, VERSION_HISTORY, COMPLEX_UNSUPPORTED ->
                new ComplexRef(expect(type, raw, Number.class).intValue());
        };
    }

    /** The reverse of {@link #canonical} for key lookups: the value Jackcess expects for an index entry. */
    public static Object toJackcess(AccessType type, Object canonical) {
        if (canonical == null) {
            return null;
        }
        return switch (canonical) {
            case Integer i when type == AccessType.BYTE -> (byte) i.intValue();
            case OleValue ole -> ole.raw();
            case ComplexRef ref -> ref.complexId();
            default -> canonical;
        };
    }

    /** The Access form of a GUID: braces and uppercase hex. */
    static String guid(String raw) {
        String hex = raw.strip();
        if (hex.startsWith("{") && hex.endsWith("}")) {
            hex = hex.substring(1, hex.length() - 1);
        }
        if (!hex.matches("[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}")) {
            throw new IllegalStateException("not a GUID: " + raw);
        }
        return "{" + hex.toUpperCase(Locale.ROOT) + "}";
    }

    private static <T> T expect(AccessType type, Object raw, Class<T> expected) {
        if (!expected.isInstance(raw)) {
            throw new IllegalStateException("column type " + type + " expects " + expected.getSimpleName()
                    + " from Jackcess, got " + raw.getClass().getName());
        }
        return expected.cast(raw);
    }
}
