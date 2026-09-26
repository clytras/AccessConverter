package io.lytrax.accessconverter.source;

import com.healthmarketscience.jackcess.Cursor;
import com.healthmarketscience.jackcess.CursorBuilder;
import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.Database.FileFormat;
import com.healthmarketscience.jackcess.DatabaseBuilder;
import com.healthmarketscience.jackcess.DateTimeType;
import com.healthmarketscience.jackcess.Index;
import com.healthmarketscience.jackcess.IndexCursor;
import com.healthmarketscience.jackcess.Row;
import com.healthmarketscience.jackcess.Table;
import com.healthmarketscience.jackcess.TableMetaData;
import com.healthmarketscience.jackcess.crypt.CryptCodecProvider;
import com.healthmarketscience.jackcess.crypt.InvalidCredentialsException;
import com.healthmarketscience.jackcess.crypt.InvalidCryptoConfigurationException;
import io.lytrax.accessconverter.model.ColumnModel;
import io.lytrax.accessconverter.model.IndexModel;
import io.lytrax.accessconverter.model.TableModel;
import io.lytrax.accessconverter.report.Issue;
import io.lytrax.accessconverter.report.IssueCode;
import io.lytrax.accessconverter.report.Issues;
import io.lytrax.accessconverter.source.SourceException.Kind;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * A read-only Access database (03, open): Access 97 through Microsoft 365, .mdb/.mde/.accdb/.accde/.accdr, encoded
 * or encrypted ones included (through jackcess-encrypt). Linked tables are never opened by accident: they are found
 * through the catalog's metadata only (F-14), and only {@link #resolve} reads one, from a back-end it opens itself
 * under {@code --linked-root} (Jackcess's own link resolution stays disabled). Anything that can't be read fails with
 * a {@link SourceException}.
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

    /** {@code PWD=} in a link's connect string, as Access stores it for a password-protected back-end. */
    private static final Pattern LINK_PASSWORD = Pattern.compile("(?i)(?:^|;)\\s*PWD=([^;]*)");

    private final Path file;
    private final Database db;
    private final Integer codePage;
    private final Issues issues;
    private ReadListener listener = ReadListener.NONE;
    private final OpenOptions options;
    /** Resolved linked tables by their name here: the back-end and the table's name there. */
    private final Map<String, Route> routes = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    /** The back-ends opened for them, by real path, each opened once. */
    private final Map<Path, AccessSource> backends = new LinkedHashMap<>();

    private AccessSource(Path file, Database db, Integer codePage, Issues issues, OpenOptions options) {
        this.file = file;
        this.db = db;
        this.codePage = codePage;
        this.issues = issues;
        this.options = options;
    }

    private record Route(AccessSource backend, String remoteTable) {}

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
            return new AccessSource(file, db, codePage, issues, options);
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
        RowStream rows;
        if (table.isComplexChild()) {
            TableModel parent = parentOf(table);
            rows = ComplexReader.childRows(fileOf(parent), orderedCursor(parent), table, table.columns());
        } else {
            rows = RowStream.of(
                    fileOf(table), orderedCursor(table), table.columns(), complexValues, table.generatedKeyColumn());
        }
        return observed(table, rows);
    }

    private RowStream observed(TableModel table, RowStream rows) {
        return listener == ReadListener.NONE ? rows : rows.observed(listener.started(table));
    }

    /** Who hears of each table and row read from here on; {@link ReadListener#NONE} for nobody. */
    public void listen(ReadListener listener) {
        this.listener = Objects.requireNonNull(listener, "listener");
    }

    private Cursor orderedCursor(TableModel table) throws IOException {
        Table jt = localTable(table);
        Cursor cursor = null;
        // A key --add-primary-key added has no Access index: its rows are numbered in physical order
        if (table.primaryKey() != null && table.generatedKeyColumn() == null) {
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
        RowStream rows;
        if (table.isComplexChild()) {
            TableModel parent = parentOf(table);
            rows = ComplexReader.childRows(
                    fileOf(parent), CursorBuilder.createCursor(localTable(parent)), table, columns);
        } else {
            rows = RowStream.of(
                    fileOf(table),
                    CursorBuilder.createCursor(localTable(table)),
                    columns,
                    false,
                    table.generatedKeyColumn());
        }
        return observed(table, rows);
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

    // ---------------------------------------------------------------- linked tables (--linked resolve)

    /** Whether linked tables are read from their back-ends ({@code --linked resolve}). */
    public boolean resolvesLinks() {
        return options.links().resolve();
    }

    /** A linked table read from its back-end: the table there, and the file it was read from. */
    public record ResolvedLink(Table table, Path file) {}

    /**
     * Reads a table linked to another Access file from that file (D11). The back-end is looked for only in {@code
     * --linked-root} (the input's directory by default), by the file name of the stored path, which itself is never
     * opened: the exact name first, then a single match ignoring case. It is opened like the input (its own code page
     * and catalog check, {@code --charset} for an Access 97 file), with the password stored in the link, else {@code
     * --password}; what opening it reports is reported under its file name. From here on, the rows of the linked
     * table come from there.
     *
     * @return empty when the table in the back-end is itself a link: chains of links are not followed
     * @throws SourceException (LINK_UNRESOLVED) when the back-end or its table can't be found or read
     */
    public Optional<ResolvedLink> resolve(LinkedTable linked) throws IOException {
        if (linked.odbc()) {
            throw new IllegalArgumentException(linked.name() + " is linked through ODBC");
        }
        Path backendFile = backendFile(linked);
        AccessSource backend = backend(backendFile, linked);
        String remote = linked.remoteTable();
        TableMetaData meta;
        try {
            meta = remote == null ? null : backend.db.getTableMetaData(remote);
        } catch (IOException | RuntimeException e) {
            throw unresolved(
                    linked, backendFile.getFileName() + " can't be read (" + SourceException.describe(e) + ")", e);
        }
        if (meta == null || meta.isSystem()) {
            throw unresolved(linked, backendFile.getFileName() + " has no table " + remote);
        }
        if (meta.isLinked()) {
            return Optional.empty();
        }
        Table table;
        try {
            table = Objects.requireNonNull(backend.db.getTable(meta.getName()), meta.getName());
        } catch (IOException | RuntimeException e) {
            throw unresolved(
                    linked,
                    "table " + meta.getName() + " in " + backendFile.getFileName() + " can't be read ("
                            + SourceException.describe(e) + ")",
                    e);
        }
        routes.put(linked.name(), new Route(backend, meta.getName()));
        return Optional.of(new ResolvedLink(table, backend.file));
    }

    /**
     * A back-end linked tables were read from.
     *
     * @param localNames the local names of each of its tables that was read, by the table's name there
     */
    public record Backend(AccessSource source, Map<String, List<String>> localNames) {}

    /**
     * The back-ends linked tables were read from, ordered by file name. A database linked to itself is left out: its
     * relationships are this database's own.
     */
    public List<Backend> backends() {
        Map<AccessSource, Map<String, List<String>>> byBackend = new LinkedHashMap<>();
        routes.forEach((local, route) -> {
            if (route.backend != this) {
                byBackend
                        .computeIfAbsent(route.backend, b -> new TreeMap<>(String.CASE_INSENSITIVE_ORDER))
                        .computeIfAbsent(route.remoteTable, r -> new ArrayList<>())
                        .add(local);
            }
        });
        List<Backend> list = new ArrayList<>();
        byBackend.forEach((source, names) -> list.add(new Backend(source, names)));
        list.sort(Comparator.comparing(b -> b.source().file().getFileName().toString(), NAME_ORDER));
        return list;
    }

    /** The file named by the link's stored path, found in the link root only. */
    private Path backendFile(LinkedTable linked) throws SourceException {
        Path root = options.links().root() != null
                ? options.links().root()
                : file.toAbsolutePath().getParent();
        String stored = Objects.requireNonNullElse(linked.database(), "");
        String name = stored.substring(Math.max(stored.lastIndexOf('\\'), stored.lastIndexOf('/')) + 1);
        if (name.isBlank() || name.equals(".") || name.equals("..")) {
            throw unresolved(linked, "its stored path names no file");
        }
        List<Path> files;
        try (Stream<Path> entries = Files.list(root)) {
            files = entries.filter(Files::isRegularFile).sorted().toList();
        } catch (IOException e) {
            throw unresolved(
                    linked, "the directory " + root + " can't be read (" + SourceException.describe(e) + ")", e);
        }
        // Listed rather than looked up, so a case-insensitive file system finds the same file a Linux one does
        for (Path candidate : files) {
            if (candidate.getFileName().toString().equals(name)) {
                return candidate;
            }
        }
        List<String> ignoringCase = files.stream()
                .map(f -> f.getFileName().toString())
                .filter(n -> n.equalsIgnoreCase(name))
                .toList();
        if (ignoringCase.size() == 1) {
            return root.resolve(ignoringCase.get(0));
        }
        if (ignoringCase.size() > 1) {
            throw unresolved(
                    linked,
                    "several files in " + root + " match " + name + " ignoring case (" + String.join(", ", ignoringCase)
                            + "); keep only the back-end there");
        }
        throw unresolved(linked, "there is no file named " + name + " in " + root + " (--linked-root)");
    }

    /** The back-end in {@code backendFile}, opened once; this database itself when it links to itself. */
    private AccessSource backend(Path backendFile, LinkedTable linked) throws IOException {
        Path real = backendFile.toRealPath();
        if (Files.isSameFile(real, file)) {
            return this;
        }
        AccessSource open = backends.get(real);
        if (open != null) {
            return open;
        }
        String password = linkPassword(linked.name()).orElse(options.password());
        Issues own = new Issues();
        AccessSource backend;
        try {
            backend = open(real, new OpenOptions(password, options.charset(), OpenOptions.Links.SKIP), own);
        } catch (SourceException e) {
            throw unresolved(linked, "its back-end can't be read: " + e.getMessage(), e);
        }
        backends.put(real, backend);
        // Said under the back-end's file name, so they don't merge with the same issue of this database
        String label = real.getFileName().toString();
        for (Issue issue : own.list()) {
            issues.add(
                    issue.code(),
                    issue.table(),
                    issue.object() != null ? issue.object() : label,
                    label + " (linked): " + issue.message());
        }
        return backend;
    }

    /** The password a link stores for its back-end ({@code MS Access;PWD=...;}), if any. Never printed. */
    private Optional<String> linkPassword(String linkName) throws IOException {
        Optional<Table> objects = systemTable("MSysObjects");
        if (objects.isEmpty()) {
            return Optional.empty();
        }
        for (Row row : objects.get()) {
            if (linkName.equals(row.get("Name")) && row.get("Connect") instanceof String connect) {
                Matcher password = LINK_PASSWORD.matcher(connect);
                return password.find() ? Optional.of(password.group(1)) : Optional.empty();
            }
        }
        return Optional.empty();
    }

    private SourceException unresolved(LinkedTable linked, String why) {
        return unresolved(linked, why, null);
    }

    private SourceException unresolved(LinkedTable linked, String why, Throwable cause) {
        return SourceException.linkUnresolved(
                file,
                "linked table " + linked.name() + " (" + linked.remoteTable() + " in " + linked.database()
                        + ") can't be read: " + why,
                cause);
    }

    @Override
    public void close() throws IOException {
        IOException failed = null;
        for (AccessSource backend : backends.values()) {
            try {
                backend.close();
            } catch (IOException e) {
                failed = e;
            }
        }
        db.close();
        if (failed != null) {
            throw failed;
        }
    }

    /** The file a table's rows are read from: a resolved linked table's back-end, or this database. */
    private Path fileOf(TableModel table) throws IOException {
        Route route = route(table.name());
        return route != null ? route.backend.file : file;
    }

    /**
     * Where a linked table's rows come from, resolving it on first use, so a source opened only to read rows (verify
     * reopens one) needs no extraction first; null for a table of this database.
     */
    private Route route(String name) throws IOException {
        Route route = routes.get(name);
        if (route == null && resolvesLinks()) {
            TableMetaData meta = db.getTableMetaData(name);
            if (meta != null && meta.getType() == TableMetaData.Type.LINKED) {
                resolve(new LinkedTable(meta.getName(), meta.getLinkedDbName(), meta.getLinkedTableName(), false));
                route = routes.get(name);
            }
        }
        return route;
    }

    private Table localTable(TableModel table) throws IOException {
        if (table.isComplexChild()) {
            throw new IllegalArgumentException(table.name() + " is made of complex values, not an Access table");
        }
        if (table.isLinked()) {
            throw new IllegalArgumentException("linked table " + table.name() + " is not read");
        }
        // A resolved linked table's rows come from its back-end, under the table's name there
        Route route = route(table.name());
        if (route != null) {
            return Objects.requireNonNull(route.backend.db.getTable(route.remoteTable), route.remoteTable);
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
