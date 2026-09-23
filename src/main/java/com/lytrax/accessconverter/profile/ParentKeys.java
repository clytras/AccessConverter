package com.lytrax.accessconverter.profile;

import com.lytrax.accessconverter.model.ColumnModel;
import com.lytrax.accessconverter.model.IndexModel;
import com.lytrax.accessconverter.model.TableModel;
import com.lytrax.accessconverter.source.AccessSource;
import com.lytrax.accessconverter.source.KeyLookup;
import com.lytrax.accessconverter.source.RowStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.text.Collator;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

/**
 * Finds a child row's parent key, returning the parent's stored key so exact and inexact matches can be told
 * apart. Normally this is a seek through the parent's Access index (04). If Jackcess can't seek it, typically for a
 * text collation it has no index codes for, the parent's keys are read once into a hash set instead. Matching then
 * follows Access's text comparison: case-insensitive, accent-sensitive, trailing spaces ignored.
 */
final class ParentKeys {
    private final AccessSource source;
    private final TableModel parent;
    private final IndexModel key;
    private final Collator text;
    private KeyLookup seek;
    private Map<List<Object>, Object[]> scanned;
    private String fallbackReason;

    private ParentKeys(AccessSource source, TableModel parent, IndexModel key) {
        this.source = source;
        this.parent = parent;
        this.key = key;
        this.text = Collator.getInstance(Locale.ROOT);
        this.text.setStrength(Collator.SECONDARY);
    }

    static ParentKeys of(AccessSource source, TableModel parent, IndexModel key) throws IOException {
        ParentKeys keys = new ParentKeys(source, parent, key);
        try {
            keys.seek = source.keyLookup(parent, key);
        } catch (RuntimeException e) {
            keys.fallBack(e);
        }
        return keys;
    }

    /** The stored parent key matching {@code childKey} (in index column order), or empty for an orphan. */
    Optional<Object[]> find(Object[] childKey) throws IOException {
        if (seek != null) {
            try {
                return seek.find(childKey);
            } catch (RuntimeException e) {
                fallBack(e);
            }
        }
        return Optional.ofNullable(scanned.get(normalize(childKey)));
    }

    /** Why the index wasn't used, or null when it was. */
    String fallbackReason() {
        return fallbackReason;
    }

    private void fallBack(RuntimeException e) throws IOException {
        seek = null;
        fallbackReason = AccessSource.indexFailure(e);
        List<ColumnModel> columns = key.columnNames().stream()
                .map(name -> parent.column(name).orElseThrow())
                .toList();
        scanned = new HashMap<>();
        try {
            RowStream rows = source.scan(parent, columns);
            while (rows.hasNext()) {
                Object[] stored = rows.next();
                if (Arrays.stream(stored).noneMatch(v -> v == null)) {
                    scanned.putIfAbsent(normalize(stored), stored);
                }
            }
        } catch (UncheckedIOException u) {
            throw u.getCause();
        }
    }

    private List<Object> normalize(Object[] values) {
        return Arrays.stream(values)
                .map(v -> switch (v) {
                    case String s -> text.getCollationKey(s.stripTrailing());
                    case BigDecimal d -> d.stripTrailingZeros();
                    case Number n -> new BigDecimal(n.toString()).stripTrailingZeros();
                    case byte[] b -> ByteBuffer.wrap(b);
                    default -> v;
                })
                .toList();
    }
}
