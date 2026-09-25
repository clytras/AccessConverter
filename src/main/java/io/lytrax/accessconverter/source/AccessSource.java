package io.lytrax.accessconverter.source;

import com.healthmarketscience.jackcess.Cursor;
import com.healthmarketscience.jackcess.CursorBuilder;
import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.Database.FileFormat;
import com.healthmarketscience.jackcess.DatabaseBuilder;
import com.healthmarketscience.jackcess.DateTimeType;
import com.healthmarketscience.jackcess.Index;
import com.healthmarketscience.jackcess.IndexCursor;
import com.healthmarketscience.jackcess.Table;
import com.healthmarketscience.jackcess.TableMetaData;
import com.healthmarketscience.jackcess.crypt.CryptCodecProvider;
import com.healthmarketscience.jackcess.crypt.InvalidCredentialsException;
import com.healthmarketscience.jackcess.crypt.InvalidCryptoConfigurationException;
import io.lytrax.accessconverter.model.ColumnModel;
import io.lytrax.accessconverter.model.IndexModel;
import io.lytrax.accessconverter.model.TableModel;
import io.lytrax.accessconverter.report.IssueCode;
import io.lytrax.accessconverter.report.Issues;
import io.lytrax.accessconverter.source.SourceException.Kind;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

/**
 * A read-only Access database (03, open): Access 97 through Microsoft 365, .mdb/.mde/.accdb/.accde/.accdr, encoded
 * or encrypted ones included (through jackcess-encrypt). Linked tables are never opened by accident: they are found
 * through the catalog's metadata only (F-14). Anything that can't be read fails with a {@link SourceException}.
 */
public final class AccessSource implements AutoCloseable {
    /** Access object names compare case-insensitively; ties (impossible in Access) fall back to exact order. */
    public static final Comparator<String> NAME_ORDER =
            String.CASE_INSENSITIVE_ORDER.thenComparing(Comparator.naturalOrder());

    /** Access's own table of relationships, read directly because {@code getRelationships()} opens linked tables. */
    public static final String RELATIONSHIPS_TABLE = "MSysRelationships";

    /** Formats whose "database password" is only a check in Access: the data isn't encrypted by it. */
    private static final Set<FileFormat> JET_PASSWORD_FORMATS =
            EnumSet.of(FileFormat.V1997, FileFormat.V2000, FileFormat.V2003, FileFormat.GENERIC_JET4);

    private final Path file;
    private final Database db;
    private final Integer codePage;
    private final Issues issues;

    private AccessSource(Path file, Database db, Integer codePage, Issues issues) {
        this.file = file;
        this.db = db;
        this.codePage = codePage;
        this.issues = issues;
    }

    /** Opens without a password or charset override; issues raised while reading go nowhere. For tests and tools. */
    public static AccessSource open(Path file) throws IOException {
        return open(file, OpenOptions.DEFAULT, new Issues());
    }

    public static AccessSource open(Path file, OpenOptions options, Issues issues) throws IOException {
        FileSignature.check(file);
        Database db = openDatabase(file, options.password(), null, false);
        try {
            Integer codePage = null;
            Charset charset = null;
            if (db.getFileFormat() == FileFormat.V1997) {
                // Jet 3 text is in the database's code page, not in Java's default charset (UTF-8)
                codePage = Jet3CodePage.read(db);
                charset = WindowsSingleByte.of(jet3Charset(codePage, options.charset(), issues));
                db.close();
                db = openDatabase(file, options.password(), charset, false);
            } else if (options.charset() != null) {
                issues.add(
                        IssueCode.CHARSET_IGNORED,
                        null,
                        null,
                        "--charset " + options.charset().name() + " applies to Access 97 files only; this "
                                + db.getFileFormat().name() + " file stores its text as Unicode");
            }
            db = withReadableCatalog(file, db, options.password(), charset, issues);
            if (JET_PASSWORD_FORMATS.contains(db.getFileFormat())) {
                String password = db.getDatabasePassword();
                if (password != null && !password.isEmpty()) {
                    issues.add(
                            IssueCode.PASSWORD_NOT_REQUIRED,
                            null,
                            null,
                            "Access asks for a database password, but a "
                                    + db.getFileFormat().name()
                                    + " password doesn't encrypt the data: it was read without one");
                }
            }
            return new AccessSource(file, db, codePage, issues);
        } catch (IOException | RuntimeException e) {
            try {
                db.close();
            } catch (IOException suppressed) {
                e.addSuppressed(suppressed);
            }
            throw e instanceof SourceException se ? se : SourceException.readFailed(file, "the database header", e);
        }
    }

    private static Charset jet3Charset(int codePage, Charset override, Issues issues) {
        Optional<Charset> mapped = CodePages.charset(codePage);
        if (override != null) {
            issues.add(
                    IssueCode.CHARSET_OVERRIDDEN,
                    null,
                    null,
                    "text decoded as " + override.name() + " (--charset) instead of the header's code page " + codePage
                            + mapped.map(c -> " (" + c.name() + ")").orElse(""));
            return override;
        }
        if (mapped.isEmpty()) {
            issues.add(
                    IssueCode.CHARSET_UNMAPPED,
                    null,
                    null,
                    "code page " + codePage + " has no Java charset here; text decoded as " + CodePages.FALLBACK.name()
                            + ". If it looks wrong, pass --charset <name>");
            return CodePages.FALLBACK;
        }
        return mapped.get();
    }

    /**
     * Makes sure the whole catalog can be read. Jackcess 5.0.1 can't always use the {@code MSysObjects} index of a
     * database written by a non-English Access (Greek Access 97 here): {@link Database#getTableMetaData} then
     * returns null for some tables and the system tables go missing, which would silently drop them (Northwind
     * from a Greek Access 97: 3 of 8 tables, no relationships). Jackcess reads the catalog by scanning it instead
     * when asked, which costs a millisecond or two, so anything incomplete is read again that way.
     */
    private static Database withReadableCatalog(Path file, Database db, String password, Charset charset, Issues issues)
            throws IOException {
        Catalog indexed = Catalog.of(db);
        if (indexed.isComplete()) {
            return db;
        }
        Database scanned = openDatabase(file, password, charset, true);
        Catalog byScan = Catalog.of(scanned);
        if (!byScan.hasMoreThan(indexed)) {
            // The catalog itself is incomplete, not its index: keep the first database and let the extractor report
            scanned.close();
            return db;
        }
        issues.add(
                IssueCode.CATALOG_INDEX_UNUSABLE,
                null,
                null,
                "Jackcess can't use this database's catalog index (" + indexed.missingFrom(byScan)
                        + "); the catalog was read by scanning it instead");
        db.close();
        return scanned;
    }

    /** What the catalog gives up: the table names that resolve, and whether the relationship table is there. */
    private record Catalog(int names, List<String> unresolved, boolean relationships) {
        static Catalog of(Database db) throws IOException {
            Set<String> names = db.getTableNames();
            List<String> unresolved = new ArrayList<>();
            for (String name : names) {
                if (db.getTableMetaData(name) == null) {
                    unresolved.add(name);
                }
            }
            return new Catalog(names.size(), unresolved, db.getSystemTable(RELATIONSHIPS_TABLE) != null);
        }

        boolean isComplete() {
            return unresolved.isEmpty() && relationships;
        }

        boolean hasMoreThan(Catalog other) {
            return names > other.names
                    || unresolved.size() < other.unresolved.size()
                    || (relationships && !other.relationships);
        }

        /** What this catalog doesn't show but {@code complete} does, for the report. */
        String missingFrom(Catalog complete) {
            List<String> missing = new ArrayList<>();
            if (!unresolved.isEmpty() || names < complete.names) {
                missing.add((complete.names - names + unresolved.size()) + " of " + complete.names
                        + " tables can't be looked up");
            }
            if (!relationships && complete.relationships) {
                missing.add(RELATIONSHIPS_TABLE + " is invisible");
            }
            return String.join(", ", missing);
        }
    }

    private static Database openDatabase(Path file, String password, Charset charset, boolean ignoreCatalogIndex)
            throws IOException {
        DatabaseBuilder builder = new DatabaseBuilder(file)
                .setReadOnly(true)
                .setCodecProvider(new CryptCodecProvider(password))
                .setIgnoreBrokenSystemCatalogIndex(ignoreCatalogIndex);
        if (charset != null) {
            builder.setCharset(charset);
        }
        Database db;
        try {
            db = builder.open();
        } catch (InvalidCredentialsException e) {
            throw password == null
                    ? new SourceException(Kind.PASSWORD_REQUIRED, file, "the database is encrypted: pass --password", e)
                    : new SourceException(Kind.WRONG_PASSWORD, file, "the password is wrong", e);
        } catch (UnsupportedOperationException | InvalidCryptoConfigurationException e) {
            // UnsupportedOperationException is what Jackcess's UnsupportedCodecException extends
            throw new SourceException(
                    Kind.UNSUPPORTED_ENCRYPTION,
                    file,
                    "the database is encrypted with a method that can't be decrypted (" + SourceException.describe(e)
                            + ")",
                    e);
        } catch (IOException e) {
            if (e.getMessage() != null && e.getMessage().startsWith("Unsupported version")) {
                throw new SourceException(
                        Kind.UNSUPPORTED_VERSION,
                        file,
                        "unsupported Access file version (format code " + FileSignature.version(file)
                                + "); Access 97 through Microsoft 365 files are supported. Convert older (Access"
                                + " 1.x/2.0) files with Access 97-2003 first",
                        e);
            }
            throw corrupt(file, e);
        } catch (RuntimeException e) {
            throw corrupt(file, e);
        }
        // The default, but a system property can change it: pin it so no java.util.Date (and no time zone) appears
        db.setDateTimeType(DateTimeType.LOCAL_DATE_TIME);
        // The converter reads stored values only; it never evaluates Access expressions
        db.setEvaluateExpressions(false);
        return db;
    }

    private static SourceException corrupt(Path file, Exception e) {
        return new SourceException(
                Kind.CORRUPT,
                file,
                "the file is damaged or truncated (" + SourceException.describe(e)
                        + "); try Compact and Repair in Access",
                e);
    }

    public Path file() {
        return file;
    }

    public String fileFormat() throws IOException {
        return db.getFileFormat().name();
    }

    /** Access 97 only: the code page in the header. Null for Access 2000 and later, which store Unicode. */
    public Integer codePage() {
        return codePage;
    }

    /** The charset text is decoded with: the code page's (or --charset) for Access 97, UTF-16LE otherwise. */
    public Charset charset() {
        return db.getCharset();
    }

    /** Local user tables, ordered by name. Jackcess types are for the extractor only. */
    public List<Table> localTables() throws IOException {
        List<Table> tables = new ArrayList<>();
        for (String name : db.getTableNames()) {
            TableMetaData meta = db.getTableMetaData(name);
            if (meta == null) {
                // Never skipped quietly: the catalog names the table, so its data would be lost without a word
                issues.add(
                        IssueCode.TABLE_NOT_IN_CATALOG,
                        name,
                        null,
                        "the catalog names this table but doesn't resolve it, so it can't be read");
            } else if (!meta.isLinked() && !meta.isSystem()) {
                try {
                    tables.add(Objects.requireNonNull(db.getTable(name), name));
                } catch (IOException | RuntimeException e) {
                    throw SourceException.readFailed(file, "table " + name, e);
                }
            }
        }
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

    /**
     * All rows of a local table, canonical values in column order, in primary-key order when there is one (03).
     * A PK index Jackcess can't read (a collation it doesn't know) falls back to physical order, reported. A complex
     * child table's rows follow its parent's order.
     */
    public RowStream rows(TableModel table) throws IOException {
        return rows(table, false);
    }

    /**
     * As {@link #rows(TableModel)}.
     *
     * @param complexValues attachment, multi-value and version-history cells carry their values ({@code
     *     ComplexValues}), for a target that inlines them; otherwise only their complex id ({@code ComplexRef})
     */
    public RowStream rows(TableModel table, boolean complexValues) throws IOException {
        if (table.isComplexChild()) {
            return ComplexReader.childRows(file, orderedCursor(parentOf(table)), table, table.columns());
        }
        return RowStream.of(file, orderedCursor(table), table.columns(), complexValues);
    }

    private Cursor orderedCursor(TableModel table) throws IOException {
        Table jt = localTable(table);
        Cursor cursor = null;
        if (table.primaryKey() != null) {
            try {
                cursor = CursorBuilder.createCursor(jackcessIndex(jt, table.primaryKey()));
            } catch (RuntimeException e) {
                issues.add(
                        IssueCode.ROW_ORDER_PHYSICAL,
                        table.name(),
                        table.primaryKey().name(),
                        "rows in physical order: the primary-key index can't be read (" + indexFailure(e) + ")");
            }
        }
        return cursor != null ? cursor : CursorBuilder.createCursor(jt);
    }

    /** Selected columns of a local table in physical order: the profiler's targeted pass. */
    public RowStream scan(TableModel table, List<ColumnModel> columns) throws IOException {
        if (table.isComplexChild()) {
            return ComplexReader.childRows(
                    file, CursorBuilder.createCursor(localTable(parentOf(table))), table, columns);
        }
        return RowStream.of(file, CursorBuilder.createCursor(localTable(table)), columns, false);
    }

    /** The Access table a complex child's values come from, with the key that orders it. */
    private TableModel parentOf(TableModel child) {
        return new TableModel(
                child.complex().parentTable(), null, List.of(), child.complex().parentKey(), List.of(), null, null, 0);
    }

    /**
     * Finds rows by the key of a PK or unique index, with Access's own index semantics. Creating the lookup, or a
     * seek, throws an unchecked exception when Jackcess can't encode keys for the index's collation.
     */
    public KeyLookup keyLookup(TableModel table, IndexModel key) throws IOException {
        Table jt = localTable(table);
        IndexCursor cursor = CursorBuilder.createCursor(jackcessIndex(jt, key));
        List<ColumnModel> columns = key.columnNames().stream()
                .map(name -> table.column(name).orElseThrow())
                .toList();
        return new KeyLookup(cursor, columns);
    }

    /**
     * Why Jackcess can't use an index, on one line. For a collation it has no index codes for, its message embeds a
     * multi-line dump of the index: only the reason after it is kept.
     */
    public static String indexFailure(RuntimeException e) {
        String message = Objects.requireNonNullElse(e.getMessage(), "");
        int dueTo = message.indexOf(" due to ");
        String reason = dueTo >= 0
                ? message.substring(dueTo + " due to ".length())
                : message.lines().findFirst().orElse("");
        return e.getClass().getSimpleName() + (reason.isBlank() ? "" : ": " + reason.strip());
    }

    @Override
    public void close() throws IOException {
        db.close();
    }

    private Table localTable(TableModel table) throws IOException {
        if (table.isComplexChild()) {
            throw new IllegalArgumentException(table.name() + " is made of complex values, not an Access table");
        }
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
