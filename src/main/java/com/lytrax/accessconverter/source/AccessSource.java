package com.lytrax.accessconverter.source;

import com.healthmarketscience.jackcess.Cursor;
import com.healthmarketscience.jackcess.CursorBuilder;
import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.DatabaseBuilder;
import com.healthmarketscience.jackcess.DateTimeType;
import com.healthmarketscience.jackcess.Index;
import com.healthmarketscience.jackcess.IndexCursor;
import com.healthmarketscience.jackcess.Table;
import com.healthmarketscience.jackcess.TableMetaData;
import com.lytrax.accessconverter.model.ColumnModel;
import com.lytrax.accessconverter.model.IndexModel;
import com.lytrax.accessconverter.model.TableModel;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

/**
 * A read-only Access database (03, open). Linked tables are never opened by accident: local tables come from
 * {@code withLocalUserTablesOnly()}, linked ones only from the catalog's metadata (F-14).
 */
public final class AccessSource implements AutoCloseable {
    /** Access object names compare case-insensitively; ties (impossible in Access) fall back to exact order. */
    public static final Comparator<String> NAME_ORDER =
            String.CASE_INSENSITIVE_ORDER.thenComparing(Comparator.naturalOrder());

    private final Path file;
    private final Database db;

    private AccessSource(Path file, Database db) {
        this.file = file;
        this.db = db;
    }

    public static AccessSource open(Path file) throws IOException {
        Database db = new DatabaseBuilder(file).setReadOnly(true).open();
        // The default, but a system property can change it: pin it so no java.util.Date (and no time zone) appears
        db.setDateTimeType(DateTimeType.LOCAL_DATE_TIME);
        // The converter reads stored values only; it never evaluates Access expressions
        db.setEvaluateExpressions(false);
        return new AccessSource(file, db);
    }

    public Path file() {
        return file;
    }

    public String fileFormat() throws IOException {
        return db.getFileFormat().name();
    }

    /** Local user tables, ordered by name. Jackcess types are for the extractor only. */
    public List<Table> localTables() {
        List<Table> tables = new ArrayList<>();
        db.newIterable().withLocalUserTablesOnly().forEach(tables::add);
        tables.sort(Comparator.comparing(Table::getName, NAME_ORDER));
        return tables;
    }

    /** Linked tables from the catalog, ordered by name. Their targets are not opened. */
    public List<LinkedTable> linkedTables() throws IOException {
        List<LinkedTable> linked = new ArrayList<>();
        for (String name : db.getTableNames()) {
            TableMetaData meta = db.getTableMetaData(name);
            if (meta != null && meta.isLinked()) {
                boolean odbc = meta.getType() == TableMetaData.Type.LINKED_ODBC;
                linked.add(new LinkedTable(
                        meta.getName(),
                        odbc ? meta.getConnectionName() : meta.getLinkedDbName(),
                        meta.getLinkedTableName(),
                        odbc));
            }
        }
        linked.sort(Comparator.comparing(LinkedTable::name, NAME_ORDER));
        return linked;
    }

    /** System table names, case-insensitive. */
    public Set<String> systemTableNames() throws IOException {
        Set<String> names = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        names.addAll(db.getSystemTableNames());
        return names;
    }

    public Optional<Table> systemTable(String name) throws IOException {
        return Optional.ofNullable(db.getSystemTable(name));
    }

    /** All rows of a local table, canonical values in column order, in primary-key order when there is one (03). */
    public RowStream rows(TableModel table) throws IOException {
        Table jt = localTable(table);
        Cursor cursor = table.primaryKey() == null
                ? CursorBuilder.createCursor(jt)
                : CursorBuilder.createCursor(jackcessIndex(jt, table.primaryKey()));
        return new RowStream(cursor, table.columns());
    }

    /** Selected columns of a local table in physical order: the profiler's targeted pass. */
    public RowStream scan(TableModel table, List<ColumnModel> columns) throws IOException {
        return new RowStream(CursorBuilder.createCursor(localTable(table)), columns);
    }

    /** Finds rows by the key of a PK or unique index, with Access's own index semantics. */
    public KeyLookup keyLookup(TableModel table, IndexModel key) throws IOException {
        Table jt = localTable(table);
        IndexCursor cursor = CursorBuilder.createCursor(jackcessIndex(jt, key));
        List<ColumnModel> columns = key.columnNames().stream()
                .map(name -> table.column(name).orElseThrow())
                .toList();
        return new KeyLookup(cursor, columns);
    }

    @Override
    public void close() throws IOException {
        db.close();
    }

    private Table localTable(TableModel table) throws IOException {
        if (table.isLinked()) {
            throw new IllegalArgumentException("linked table " + table.name() + " is not read");
        }
        return Objects.requireNonNull(db.getTable(table.name()), () -> "no table " + table.name());
    }

    /** The Jackcess index behind a normalized index: same name (one of its sources) and same columns. */
    private static Index jackcessIndex(Table table, IndexModel model) {
        for (Index index : table.getIndexes()) {
            List<String> columns =
                    index.getColumns().stream().map(Index.Column::getName).toList();
            if (model.sourceNames().contains(index.getName()) && columns.equals(model.columnNames())) {
                return index;
            }
        }
        throw new IllegalStateException("no Access index for " + table.getName() + "." + model.name());
    }

    /** A linked table's catalog entry: the stored target, which may not exist on this machine. */
    public record LinkedTable(String name, String database, String remoteTable, boolean odbc) {}
}
