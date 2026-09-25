package io.lytrax.accessconverter.profile;

import io.lytrax.accessconverter.model.ColumnModel;
import io.lytrax.accessconverter.model.IndexModel;
import io.lytrax.accessconverter.model.TableModel;
import io.lytrax.accessconverter.source.AccessSource;
import io.lytrax.accessconverter.source.KeyLookup;
import io.lytrax.accessconverter.source.RowStream;
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
 *
 * <p>A seek that finds nothing is never trusted on its own: the parent's keys are scanned to confirm the row really
 * isn't there. Jackcess's seek misses existing rows in some databases (measured on a Greek Access 97 Northwind,
 * where 512 of 830 primary-key seeks failed while the same index iterated all 830 rows), and an orphan that isn't
 * one would cost the output a foreign key.
 */
final class ParentKeys {

    /** Looks a key up, as {@link KeyLookup} does; replaceable in tests. */
    interface Seeker {
        Optional<Object[]> find(Object[] key) throws IOException;
    }

    private final AccessSource source;
    private final TableModel parent;
    private final IndexModel key;
    private final Collator text;
    private Seeker seek;
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
            KeyLookup lookup = source.keyLookup(parent, key);
            keys.seek = lookup::find;
        } catch (RuntimeException e) {
            keys.fallbackReason = AccessSource.indexFailure(e);
            keys.scan();
        }
        return keys;
    }

    /** As {@link #of}, with the index seek replaced: lets a test make a seek miss rows that are there. */
    static ParentKeys withSeek(AccessSource source, TableModel parent, IndexModel key, Seeker seek) {
        ParentKeys keys = new ParentKeys(source, parent, key);
        keys.seek = seek;
        return keys;
    }

    /** The stored parent key matching {@code childKey} (in index column order), or empty for an orphan. */
    Optional<Object[]> find(Object[] childKey) throws IOException {
        if (seek != null) {
            try {
                Optional<Object[]> found = seek.find(childKey);
                if (found.isPresent()) {
                    return found;
                }
            } catch (RuntimeException e) {
                fallbackReason = AccessSource.indexFailure(e);
                seek = null;
            }
        }
        if (scanned == null) {
            scan();
        }
        Optional<Object[]> found = Optional.ofNullable(scanned.get(normalize(childKey)));
        if (found.isPresent() && seek != null) {
            // The index said there is no such row, and the table says there is: stop believing the index
            seek = null;
            fallbackReason = "the index seek missed rows the table holds";
        }
        return found;
    }

    /** Why the index wasn't used, or null when it was. */
    String fallbackReason() {
        return fallbackReason;
    }

    /** Reads the parent's key columns once into a hash set keyed the way Access compares keys. */
    private void scan() throws IOException {
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
                    case String s -> text.getCollationKey(RuleEvaluator.accessComparable(s));
                    case BigDecimal d -> d.stripTrailingZeros();
                    case Number n -> new BigDecimal(n.toString()).stripTrailingZeros();
                    case byte[] b -> ByteBuffer.wrap(b);
                    default -> v;
                })
                .toList();
    }
}
