package io.lytrax.accessconverter.extract;

import io.lytrax.accessconverter.model.IndexModel;
import io.lytrax.accessconverter.model.IndexModel.IndexColumn;
import io.lytrax.accessconverter.model.IndexModel.Origin;
import io.lytrax.accessconverter.report.IssueCode;
import io.lytrax.accessconverter.report.Issues;
import io.lytrax.accessconverter.source.AccessSource;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Turns Access's logical indexes into the indexes a target should create (04, Index normalization; F-30). Uses
 * public Jackcess API only, through {@link RawIndex}.
 *
 * <ul>
 *   <li>The primary key is kept.
 *   <li>User indexes are kept, except exact duplicates (same signature) and non-unique indexes whose ordered
 *       columns equal a kept PK or unique index: they add no constraint and no lookup path.
 *   <li>Unique relationship indexes (the referenced side, {@code .rB}, or a one-to-one child side) are dropped
 *       when their columns are a kept PK or unique index again; otherwise they're kept, as Access enforces them.
 *   <li>Non-unique relationship indexes (the child side) are kept under the relationship's name, unless a kept
 *       index already starts with the same columns.
 * </ul>
 *
 * Signature = ordered (column, ascending) pairs + unique + ignoreNulls. Merged Access names are kept in
 * {@link IndexModel#sourceNames()}.
 */
public final class IndexNormalizer {
    private IndexNormalizer() {}

    /** A logical index as Jackcess reports it. */
    public record RawIndex(
            String name,
            List<IndexColumn> columns,
            boolean primaryKey,
            boolean foreignKey,
            boolean unique,
            boolean ignoreNulls,
            boolean required) {

        public RawIndex {
            columns = List.copyOf(columns);
        }
    }

    public record Result(IndexModel primaryKey, List<IndexModel> indexes) {}

    public static Result normalize(String table, List<RawIndex> raw, Issues issues) {
        List<Builder> kept = new ArrayList<>();
        RawIndex pkSource =
                raw.stream().filter(RawIndex::primaryKey).findFirst().orElse(null);
        Builder pk = null;
        if (pkSource != null) {
            pk = new Builder(pkSource, Origin.USER);
            pk.primaryKey = true;
            pk.unique = true;
            pk.required = true;
            kept.add(pk);
        }

        // Unique user indexes first, so a plain index on the same columns can be absorbed whatever the names
        List<RawIndex> user = raw.stream()
                .filter(i -> !i.foreignKey() && i != pkSource)
                .sorted(Comparator.comparing(RawIndex::unique)
                        .reversed()
                        .thenComparing(RawIndex::name, AccessSource.NAME_ORDER)
                        .thenComparing(i -> describe(i.columns())))
                .toList();
        for (RawIndex index : user) {
            Builder same = find(kept, k -> k.sameSignature(index));
            if (same != null) {
                same.absorb(index);
                issues.add(
                        IssueCode.INDEX_MERGED_DUPLICATE,
                        table,
                        index.name(),
                        "duplicate of index " + same.name + " (same columns, order and flags); merged");
                continue;
            }
            Builder stronger = index.unique() ? null : find(kept, k -> k.unique && k.sameColumns(index.columns()));
            if (stronger != null) {
                stronger.absorb(index);
                issues.add(
                        IssueCode.INDEX_MERGED_DUPLICATE,
                        table,
                        index.name(),
                        "plain index on the same columns as " + (stronger.primaryKey ? "the primary key " : "unique ")
                                + stronger.name + "; merged");
                continue;
            }
            kept.add(new Builder(index, Origin.USER));
        }

        for (RawIndex index : raw) {
            if (!index.foreignKey() || !index.unique()) {
                continue;
            }
            Builder key = find(kept, k -> k.unique && k.sameColumnSet(index.columns()));
            if (key != null) {
                issues.add(
                        IssueCode.INDEX_BACKING_DROPPED,
                        table,
                        index.name(),
                        "hidden relationship index on " + describe(index.columns()) + " repeats "
                                + (key.primaryKey ? "the primary key " : "unique index ") + key.name + "; dropped");
            } else {
                kept.add(new Builder(index, Origin.RELATIONSHIP));
            }
        }

        for (RawIndex index : raw) {
            if (!index.foreignKey() || index.unique()) {
                continue;
            }
            Builder covering = find(kept, k -> k.startsWith(index.columns()));
            if (covering != null) {
                covering.absorb(index);
                issues.add(
                        IssueCode.INDEX_MERGED_DUPLICATE,
                        table,
                        index.name(),
                        "hidden relationship index on " + describe(index.columns()) + " is covered by index "
                                + covering.name + "; merged");
            } else {
                kept.add(new Builder(index, Origin.RELATIONSHIP));
            }
        }

        IndexModel primaryKey = pk == null ? null : pk.build();
        List<IndexModel> indexes = kept.stream()
                .filter(k -> !k.primaryKey)
                .map(Builder::build)
                .sorted(Comparator.comparing(IndexModel::name, AccessSource.NAME_ORDER)
                        .thenComparing(i -> describe(i.columns())))
                .toList();
        return new Result(primaryKey, indexes);
    }

    static String describe(List<IndexColumn> columns) {
        return columns.stream()
                .map(c -> c.name() + (c.ascending() ? "" : " DESC"))
                .collect(Collectors.joining(", ", "(", ")"));
    }

    private static Builder find(List<Builder> kept, Predicate<Builder> test) {
        return kept.stream().filter(test).findFirst().orElse(null);
    }

    private static final class Builder {
        final String name;
        final List<IndexColumn> columns;
        final Origin origin;
        final boolean ignoreNulls;
        final List<String> sourceNames = new ArrayList<>();
        boolean primaryKey;
        boolean unique;
        boolean required;

        Builder(RawIndex index, Origin origin) {
            this.name = index.name();
            this.columns = index.columns();
            this.origin = origin;
            this.unique = index.unique();
            this.ignoreNulls = index.ignoreNulls();
            this.required = index.required();
            sourceNames.add(index.name());
        }

        void absorb(RawIndex index) {
            required |= index.required();
            if (sourceNames.stream().noneMatch(n -> n.equals(index.name()))) {
                sourceNames.add(index.name());
            }
        }

        boolean sameSignature(RawIndex index) {
            return unique == index.unique() && ignoreNulls == index.ignoreNulls() && sameColumns(index.columns());
        }

        boolean sameColumns(List<IndexColumn> other) {
            if (columns.size() != other.size()) {
                return false;
            }
            for (int i = 0; i < columns.size(); i++) {
                IndexColumn a = columns.get(i);
                IndexColumn b = other.get(i);
                if (!a.name().equalsIgnoreCase(b.name()) || a.ascending() != b.ascending()) {
                    return false;
                }
            }
            return true;
        }

        /** Uniqueness holds for a set of columns whatever their order or direction. */
        boolean sameColumnSet(List<IndexColumn> other) {
            return names(columns).equals(names(other));
        }

        /** Lookups on {@code prefix} can use this index: its leading columns are the same, in order. */
        boolean startsWith(List<IndexColumn> prefix) {
            if (prefix.size() > columns.size()) {
                return false;
            }
            for (int i = 0; i < prefix.size(); i++) {
                if (!columns.get(i).name().equalsIgnoreCase(prefix.get(i).name())) {
                    return false;
                }
            }
            return true;
        }

        IndexModel build() {
            return new IndexModel(name, columns, primaryKey, unique, ignoreNulls, required, origin, sourceNames);
        }

        private static Set<String> names(List<IndexColumn> columns) {
            Set<String> names = new TreeSet<>();
            columns.forEach(c -> names.add(c.name().toLowerCase(Locale.ROOT)));
            return names;
        }
    }
}
