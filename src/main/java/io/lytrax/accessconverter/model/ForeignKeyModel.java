package io.lytrax.accessconverter.model;

import java.util.List;
import java.util.Objects;

/**
 * An Access relationship, decoded from {@code MSysRelationships} (04, Relationships).
 *
 * @param parentTable the referenced ("one") side
 * @param childTable the side holding the foreign-key columns
 * @param enforced "Enforce Referential Integrity" in Access. Without it Access allows orphans (F-31).
 * @param flags the raw {@code grbit} value
 * @param parentKey the parent's PK or unique index whose columns equal {@code parentColumns}, or null
 */
public record ForeignKeyModel(
        String name,
        String parentTable,
        List<String> parentColumns,
        String childTable,
        List<String> childColumns,
        boolean enforced,
        Action onUpdate,
        Action onDelete,
        boolean oneToOne,
        Join join,
        int flags,
        Status status,
        String parentKey) {

    public enum Action {
        NO_ACTION,
        CASCADE,
        SET_NULL
    }

    public enum Join {
        INNER,
        LEFT_OUTER,
        RIGHT_OUTER
    }

    public enum Status {
        /** Enforced, both tables local, the parent columns are a key: a candidate foreign key. */
        EMIT,
        /** A join line only. Never a constraint; targets keep the child-side index. */
        NOT_ENFORCED,
        SKIPPED_LINKED_TABLE,
        SKIPPED_TABLE_EXCLUDED,
        /** Enforced, but the parent columns aren't the parent's PK or a unique index. */
        SKIPPED_PARENT_KEY_MISSING,
        /** The {@code MSysRelationships} rows are inconsistent, or name a column or table that doesn't exist. */
        SKIPPED_MALFORMED
    }

    public ForeignKeyModel {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(status, "status");
        parentColumns = List.copyOf(parentColumns);
        childColumns = List.copyOf(childColumns);
        if (parentColumns.size() != childColumns.size()) {
            throw new IllegalArgumentException("relationship " + name + " pairs " + parentColumns.size()
                    + " parent columns with " + childColumns.size() + " child columns");
        }
    }

    public boolean isSelfReferencing() {
        return parentTable.equalsIgnoreCase(childTable);
    }
}
