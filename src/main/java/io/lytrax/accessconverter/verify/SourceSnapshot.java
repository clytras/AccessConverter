package io.lytrax.accessconverter.verify;

import io.lytrax.accessconverter.model.SchemaModel;
import io.lytrax.accessconverter.model.TableModel;
import io.lytrax.accessconverter.source.AccessSource;
import io.lytrax.accessconverter.source.RowStream;
import io.lytrax.accessconverter.value.CanonicalText;
import io.lytrax.accessconverter.value.ComplexRef;
import io.lytrax.accessconverter.value.OleValue;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;

/**
 * The source side of {@code verify} (09): every local table's row count and a digest of its canonical rows in
 * primary-key order. Equal digests mean equal data, independent of OS, locale and time zone.
 */
public record SourceSnapshot(SchemaModel.Source source, List<TableSnapshot> tables) {

    public SourceSnapshot {
        tables = List.copyOf(tables);
    }

    /** @param sha256 digest of the rows' canonical encoding, in stream order */
    public record TableSnapshot(String table, long rows, String sha256) {}

    public static SourceSnapshot capture(AccessSource source, SchemaModel model) throws IOException {
        List<TableSnapshot> tables = new ArrayList<>();
        for (TableModel table : model.tables()) {
            if (table.isLinked()) {
                continue;
            }
            MessageDigest digest = sha256();
            long rows = 0;
            RowStream stream = source.rows(table);
            while (stream.hasNext()) {
                for (Object value : stream.next()) {
                    digest.update(encode(value).getBytes(StandardCharsets.UTF_8));
                    digest.update((byte) 0x1F);
                }
                digest.update((byte) 0x1E);
                rows++;
            }
            tables.add(new TableSnapshot(table.name(), rows, HexFormat.of().formatHex(digest.digest())));
        }
        return new SourceSnapshot(model.source(), tables);
    }

    /**
     * A type-tagged, length-prefixed text form of a canonical value. Integer types share a tag, and decimals are
     * encoded by value (trailing zeros stripped), so the encoding identifies values rather than Java types.
     */
    static String encode(Object value) {
        return switch (value) {
            case null -> "N";
            case Boolean b -> b ? "Z1" : "Z0";
            case Short s -> "I" + s;
            case Integer i -> "I" + i;
            case Long l -> "I" + l;
            case BigDecimal d -> "D" + d.stripTrailingZeros().toPlainString();
            case Float f -> "F" + f;
            case Double d -> "R" + d;
            case LocalDateTime t -> "T" + CanonicalText.dateTime(t);
            case String s -> "S" + s.length() + ":" + s;
            case byte[] b -> "B" + CanonicalText.sha256(b);
            case OleValue ole -> "O" + CanonicalText.sha256(ole.raw());
            case ComplexRef ref -> "C" + ref.complexId();
            default ->
                throw new IllegalArgumentException(
                        "not a canonical value: " + value.getClass().getName());
        };
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("every JDK provides SHA-256", e);
        }
    }
}
