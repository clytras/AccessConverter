package io.lytrax.accessconverter.extract;

import io.lytrax.accessconverter.source.AccessSource;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * Decodes the rows of the {@code MSysRelationships} system table (04, Relationships). It reads the table directly
 * because {@code Database.getRelationships()} opens both tables of every relationship, which fails as soon as one
 * of them is linked (F-14).
 *
 * <p>Each row is one column pair of a relationship: rows are grouped by {@code szRelationship} (ignoring case, like
 * Access names), columns are ordered by {@code icolumn}. {@code szReferencedObject}/{@code szReferencedColumn} are
 * the parent, {@code szObject}/{@code szColumn} the child.
 */
public final class RelationshipDecoder {
    public static final int ONE_TO_ONE = 0x00000001;
    public static final int NO_REFERENTIAL_INTEGRITY = 0x00000002;
    public static final int CASCADE_UPDATES = 0x00000100;
    public static final int CASCADE_DELETES = 0x00001000;
    public static final int CASCADE_NULL = 0x00002000;
    public static final int LEFT_OUTER_JOIN = 0x01000000;
    public static final int RIGHT_OUTER_JOIN = 0x02000000;

    static final String NAME = "szRelationship";
    static final String CHILD_TABLE = "szObject";
    static final String CHILD_COLUMN = "szColumn";
    static final String PARENT_TABLE = "szReferencedObject";
    static final String PARENT_COLUMN = "szReferencedColumn";
    static final String COLUMN_INDEX = "icolumn";
    static final String COLUMN_COUNT = "ccolumn";
    static final String FLAGS = "grbit";

    private RelationshipDecoder() {}

    /**
     * One relationship as stored.
     *
     * @param problem why the rows are inconsistent, or null when they're fine
     */
    public record Decoded(
            String name,
            String parentTable,
            String childTable,
            List<String> parentColumns,
            List<String> childColumns,
            int flags,
            String problem) {

        public Decoded {
            parentColumns = List.copyOf(parentColumns);
            childColumns = List.copyOf(childColumns);
        }

        public boolean has(int flag) {
            return (flags & flag) != 0;
        }
    }

    /** Relationships ordered by name. */
    public static List<Decoded> decode(Iterable<? extends Map<String, ?>> rows) {
        Map<String, List<Map<String, ?>>> groups = new LinkedHashMap<>();
        for (Map<String, ?> row : rows) {
            String name = Objects.toString(row.get(NAME), "");
            groups.computeIfAbsent(name.toLowerCase(Locale.ROOT), k -> new ArrayList<>())
                    .add(row);
        }
        List<Decoded> decoded = new ArrayList<>(groups.size());
        groups.values().forEach(group -> decoded.add(decodeOne(group)));
        decoded.sort(Comparator.comparing(Decoded::name, AccessSource.NAME_ORDER));
        return decoded;
    }

    private static Decoded decodeOne(List<Map<String, ?>> rows) {
        Map<String, ?> first = rows.getFirst();
        String name = Objects.toString(first.get(NAME), "");
        String parentTable = string(first, PARENT_TABLE);
        String childTable = string(first, CHILD_TABLE);
        Integer count = integer(first, COLUMN_COUNT);
        Integer flags = integer(first, FLAGS);

        String problem = null;
        TreeMap<Integer, Map<String, ?>> byIndex = new TreeMap<>();
        for (Map<String, ?> row : rows) {
            Integer index = integer(row, COLUMN_INDEX);
            if (name.isEmpty()) {
                problem = "a row has no relationship name";
            } else if (parentTable == null
                    || childTable == null
                    || string(row, PARENT_COLUMN) == null
                    || string(row, CHILD_COLUMN) == null
                    || index == null
                    || count == null
                    || flags == null) {
                problem = "a row is missing a table, column, position, count or flags";
            } else if (!parentTable.equalsIgnoreCase(string(row, PARENT_TABLE))
                    || !childTable.equalsIgnoreCase(string(row, CHILD_TABLE))) {
                problem = "rows name different tables";
            } else if (!count.equals(integer(row, COLUMN_COUNT)) || !flags.equals(integer(row, FLAGS))) {
                problem = "rows disagree on the column count or flags";
            } else if (byIndex.put(index, row) != null) {
                problem = "column position " + index + " appears twice";
            }
            if (problem != null) {
                break;
            }
        }
        if (problem == null && (byIndex.size() != count || byIndex.firstKey() != 0 || byIndex.lastKey() != count - 1)) {
            problem = "expected column positions 0.." + (count - 1) + ", found " + byIndex.keySet();
        }

        List<String> parentColumns = new ArrayList<>();
        List<String> childColumns = new ArrayList<>();
        for (Map<String, ?> row : byIndex.values()) {
            parentColumns.add(string(row, PARENT_COLUMN));
            childColumns.add(string(row, CHILD_COLUMN));
        }
        return new Decoded(
                name, parentTable, childTable, parentColumns, childColumns, flags == null ? 0 : flags, problem);
    }

    private static String string(Map<String, ?> row, String column) {
        Object value = row.get(column);
        return value == null ? null : value.toString();
    }

    private static Integer integer(Map<String, ?> row, String column) {
        return row.get(column) instanceof Number n ? n.intValue() : null;
    }
}
