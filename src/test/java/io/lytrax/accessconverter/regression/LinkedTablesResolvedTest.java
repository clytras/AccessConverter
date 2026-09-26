package io.lytrax.accessconverter.regression;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.healthmarketscience.jackcess.ColumnBuilder;
import com.healthmarketscience.jackcess.Cursor;
import com.healthmarketscience.jackcess.CursorBuilder;
import com.healthmarketscience.jackcess.DataType;
import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.DatabaseBuilder;
import com.healthmarketscience.jackcess.IndexBuilder;
import com.healthmarketscience.jackcess.RelationshipBuilder;
import com.healthmarketscience.jackcess.Table;
import com.healthmarketscience.jackcess.TableBuilder;
import io.lytrax.accessconverter.Extraction;
import io.lytrax.accessconverter.extract.ExtractOptions;
import io.lytrax.accessconverter.extract.SchemaExtractor;
import io.lytrax.accessconverter.fixtures.Access97Fixture;
import io.lytrax.accessconverter.fixtures.CorpusFile;
import io.lytrax.accessconverter.fixtures.Fixtures;
import io.lytrax.accessconverter.fixtures.GuestDump;
import io.lytrax.accessconverter.fixtures.GuestDump.DumpTable;
import io.lytrax.accessconverter.model.AccessType;
import io.lytrax.accessconverter.model.ColumnModel;
import io.lytrax.accessconverter.model.ForeignKeyModel;
import io.lytrax.accessconverter.model.ForeignKeyModel.Status;
import io.lytrax.accessconverter.model.TableModel;
import io.lytrax.accessconverter.report.Issue;
import io.lytrax.accessconverter.report.IssueCode;
import io.lytrax.accessconverter.report.Issues;
import io.lytrax.accessconverter.source.AccessSource;
import io.lytrax.accessconverter.source.OpenOptions;
import io.lytrax.accessconverter.source.SourceException;
import io.lytrax.accessconverter.target.ConvertOptions;
import io.lytrax.accessconverter.target.json.JsonFixture;
import io.lytrax.accessconverter.target.json.JsonOptions;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * D11: with {@code --linked resolve}, a table linked to another Access file is read from that file, found by its file
 * name in the link root and never at the path Access stored, and is then a table like any other, under the name the
 * linking database gives it. The back-end's own relationships between the tables read from it come along.
 */
class LinkedTablesResolvedTest {

    @TempDir
    Path dir;

    private static OpenOptions resolve(Path root) {
        return new OpenOptions(null, null, OpenOptions.Links.resolve(root));
    }

    private static Extraction extract(Path file, OpenOptions options) {
        return Extraction.of(file, options);
    }

    // ---------------------------------------------------------------- linkedV2007 (tier B)

    /**
     * linkedV2007's Table2 is linked to Table1 of linkeeTest, stored as Z:\jackcess_test\linkeeTest.accdb, while
     * linkedV2007 has a Table1 of its own: the table read is named Table2, as the linking database names it.
     */
    @Test
    void aTableLinkedToAnotherFileIsReadFromItUnderItsLocalName() throws IOException {
        Path front = CorpusFile.get("jackcess/V2007/linkedV2007.accdb").file();
        Path back = CorpusFile.get("jackcess/linkeeTest.accdb").file();
        Extraction extraction = extract(front, resolve(back.getParent()));

        TableModel linked = extraction.table("Table2");
        assertThat(linked.isLinked()).isFalse();
        assertThat(linked.isResolvedLink()).isTrue();
        assertThat(linked.link().database()).isEqualTo("Z:\\jackcess_test\\linkeeTest.accdb");
        assertThat(linked.link().remoteTable()).isEqualTo("Table1");
        assertThat(Path.of(linked.link().readFrom())).isEqualTo(back);
        assertThat(linked.columns()).extracting(ColumnModel::name).containsExactly("ID", "Field1");
        assertThat(extraction.table("Table1").isResolvedLink()).isFalse();
        assertThat(extraction.issues(IssueCode.LINKED_TABLE_SKIPPED)).isEmpty();
        assertThat(extraction.issues(IssueCode.LINKED_TABLE_RESOLVED))
                .singleElement()
                .satisfies(i -> assertThat(i.message()).contains("Table1", back.toString()));
        // Access can't enforce a relationship across files: it is a join line, as between two local tables
        assertThat(extraction.relationship("Table1Table2").status()).isEqualTo(Status.NOT_ENFORCED);

        try (AccessSource source = AccessSource.open(front, resolve(back.getParent()), new Issues())) {
            TableModel table = SchemaExtractor.extract(source, ExtractOptions.ALL, new Issues())
                    .table("Table2")
                    .orElseThrow();
            List<Object[]> rows = new ArrayList<>();
            source.rows(table).forEachRemaining(rows::add);
            assertThat(rows).singleElement().satisfies(r -> assertThat(r).containsExactly(1, "bar"));
        }
    }

    /** The link root defaults to the input's directory; linkeeTest isn't in V2007/, so it can't be found there. */
    @Test
    void aBackEndThatIsNotInTheRootIsATableError() {
        Path front = CorpusFile.get("jackcess/V2007/linkedV2007.accdb").file();
        assertThatThrownBy(() -> extract(front, resolve(null)))
                .rootCause()
                .isInstanceOf(SourceException.class)
                .satisfies(
                        e -> assertThat(((SourceException) e).kind()).isEqualTo(SourceException.Kind.LINK_UNRESOLVED))
                .hasMessageContaining("linked table Table2")
                .hasMessageContaining("no file named linkeeTest.accdb in " + front.getParent());

        // With --on-table-error continue it is an error in the report, and the table stays unread
        Issues issues = new Issues();
        try (AccessSource source = AccessSource.open(front, resolve(null), issues)) {
            var model = SchemaExtractor.extract(source, ExtractOptions.ALL.continueOnTableError(true), issues);
            assertThat(model.table("Table2").orElseThrow().isLinked()).isTrue();
        } catch (IOException e) {
            throw new AssertionError(e);
        }
        assertThat(issues.list())
                .filteredOn(i -> i.code() == IssueCode.TABLE_READ_FAILED)
                .singleElement()
                .satisfies(i -> assertThat(i.table()).isEqualTo("Table2"));
    }

    /**
     * An ODBC link has no file to read: it is skipped and reported as without resolve, and the JSON export that keeps
     * its connection string still warns about the password in it.
     */
    @Test
    void anOdbcLinkIsStillSkipped() {
        Path front = CorpusFile.get("jackcess/V2007/linkedOdbcV2007.accdb").file();
        Extraction extraction = extract(front, resolve(null));
        assertThat(extraction.table("Ordrar").isLinked()).isTrue();
        assertThat(extraction.issues(IssueCode.LINKED_TABLE_SKIPPED)).hasSize(1);
        assertThat(extraction.issues(IssueCode.LINKED_TABLE_RESOLVED)).isEmpty();

        var exported = JsonFixture.convert(
                front, dir.resolve("odbc.json"), resolve(null), ConvertOptions.DEFAULT, JsonOptions.DEFAULT);
        assertThat(exported.issues(IssueCode.LINKED_CONNECTION_PASSWORD))
                .singleElement()
                .satisfies(i -> assertThat(i.table()).isEqualTo("Ordrar"));
    }

    // ---------------------------------------------------------------- Access 97 (tier D)

    /**
     * The Greek Access 97 pair: the stored path is C:\WORK\LINKBACK97.MDB, the file here linkBack97.mdb (found
     * ignoring case), in code page 1253 with an index Jackcess can't use, so the back-end gets its own code page and
     * its own catalog check. Every value of the linked tables is what Access itself read through the link.
     */
    @Test
    void theAccess97LinkedTablesHoldWhatAccessReadsThroughTheLink() throws IOException {
        Path front = Access97Fixture.LINK_FRONT.file();
        GuestDump dump = Access97Fixture.LINK_FRONT.dump();
        Extraction extraction = extract(front, resolve(null));

        assertThat(extraction.model().tables())
                .extracting(TableModel::name)
                .containsExactlyElementsOf(
                        dump.tables().stream().map(DumpTable::name).toList());
        for (String name : List.of("Είδη", "Κατηγορίες")) {
            TableModel table = extraction.table(name);
            DumpTable dumped = dump.table(name);
            assertThat(table.isResolvedLink()).as(name).isTrue();
            assertThat(table.columns()).extracting(ColumnModel::name).containsExactlyElementsOf(dumped.fields());
            List<Object[]> rows = rows(front, table);
            assertThat(rows).as(name).hasSize(dumped.rowCount());
            for (int r = 0; r < rows.size(); r++) {
                for (int c = 0; c < table.columns().size(); c++) {
                    assertValue(
                            rows.get(r)[c],
                            dumped.rows().get(r).get(c),
                            table.columns().get(c).type(),
                            name + " row " + (r + 1) + " column " + c);
                }
            }
        }
        assertThat(extraction.table("Είδη").link().remoteTable()).isEqualTo("Προϊόντα");
        assertThat(Path.of(extraction.table("Είδη").link().readFrom())).isEqualTo(Access97Fixture.LINK_BACK.file());
        // The front-end's own Suppliers is its own, not the back-end's
        assertThat(extraction.table("Suppliers").columns())
                .extracting(ColumnModel::name)
                .containsExactly("SupplierID", "Επωνυμία");

        // What opening the back-end said is said under its name, apart from the same issue of the front-end
        assertThat(extraction.issues(IssueCode.CATALOG_INDEX_UNUSABLE))
                .extracting(Issue::object)
                .containsExactlyInAnyOrder(null, "linkBack97.mdb");
        assertThat(extraction.issues(IssueCode.PASSWORD_NOT_REQUIRED))
                .singleElement()
                .satisfies(i -> assertThat(i.message()).startsWith("linkBack97.mdb (linked): "));
    }

    /**
     * The back-end's enforced ΚατηγορίεςΠροϊόντα joins two tables that are both read, so it becomes a foreign key
     * between them under their names here. SuppliersΠροϊόντα reaches a table that isn't linked: it is left out and
     * reported, and never attached to the front-end's own Suppliers. The front-end's relationship to a linked table
     * is the join line Access stored.
     */
    @Test
    void theBackEndsRelationshipsComeAlongUnderTheLocalNames() {
        Extraction extraction = extract(Access97Fixture.LINK_FRONT.file(), resolve(null));
        GuestDump back = Access97Fixture.LINK_BACK.dump();

        ForeignKeyModel categories = extraction.relationship("ΚατηγορίεςΠροϊόντα");
        GuestDump.DumpRelation dumped = back.relation("ΚατηγορίεςΠροϊόντα").orElseThrow();
        assertThat(categories.parentTable()).isEqualTo("Κατηγορίες");
        assertThat(categories.childTable()).isEqualTo("Είδη");
        assertThat(categories.parentColumns()).containsExactly(dumped.parentColumn());
        assertThat(categories.childColumns()).containsExactly(dumped.childColumn());
        assertThat(categories.status()).isEqualTo(Status.EMIT);
        assertThat(categories.onUpdate()).isEqualTo(ForeignKeyModel.Action.CASCADE);

        assertThat(extraction.relationship("ΕίδηOrders").status()).isEqualTo(Status.NOT_ENFORCED);
        assertThat(extraction.model().relationships())
                .extracting(ForeignKeyModel::name)
                .containsExactly("ΕίδηOrders", "ΚατηγορίεςΠροϊόντα");
        assertThat(extraction.issues(IssueCode.FK_SKIPPED_TABLE_EXCLUDED))
                .singleElement()
                .satisfies(i -> {
                    assertThat(i.object()).isEqualTo("SuppliersΠροϊόντα");
                    assertThat(i.message()).contains("linkBack97.mdb", "Suppliers", "isn't read");
                });
    }

    /** Without --linked resolve nothing changes: the links are skipped and reported, and the message says why. */
    @Test
    void withoutResolveTheLinksAreSkippedAsBefore() {
        Extraction extraction = Extraction.of(Access97Fixture.LINK_FRONT.file());
        assertThat(extraction.table("Είδη").isLinked()).isTrue();
        assertThat(extraction.issues(IssueCode.LINKED_TABLE_SKIPPED))
                .hasSize(2)
                .allSatisfy(i -> assertThat(i.message()).contains("--linked resolve"));
    }

    // ---------------------------------------------------------------- generated pairs

    /**
     * An encrypted back-end (Office 2013 encryption) opens with the password its link stores, as Access does, or
     * with --password when the link stores none. The password never reaches a message.
     */
    @Test
    void anEncryptedBackEndOpensWithTheLinksPasswordElseTheGivenOne() throws IOException {
        CorpusFile encrypted = CorpusFile.get("jackcess-encrypt/db2013-enc.accdb");
        Path back = Files.copy(encrypted.file(), dir.resolve("secret.accdb"));
        String remote;
        try (Database db = Fixtures.openReadOnly(back, encrypted.password())) {
            remote = db.getTableNames().iterator().next();
        }
        Path front = dir.resolve("front.accdb");
        try (Database db = create(front)) {
            db.createLinkedTable("Secret", "\\\\server\\share\\secret.accdb", remote);
        }

        assertThatThrownBy(() -> extract(front, resolve(null)))
                .hasMessageContaining("linked table Secret")
                .hasMessageContaining("pass --password");

        OpenOptions given = new OpenOptions(encrypted.password(), null, OpenOptions.Links.resolve(null));
        assertThat(extract(front, given).table("Secret").isResolvedLink()).isTrue();

        try (Database db = new DatabaseBuilder(front).open()) {
            Table objects = db.getSystemTable("MSysObjects");
            Cursor cursor = CursorBuilder.createCursor(objects);
            assertThat(cursor.findFirstRow(objects.getColumn("Name"), "Secret")).isTrue();
            cursor.setCurrentRowValue(objects.getColumn("Connect"), "MS Access;PWD=" + encrypted.password() + ";");
        }
        Extraction stored = extract(front, resolve(null));
        assertThat(stored.table("Secret").isResolvedLink()).isTrue();
        assertThat(stored.issues()).allSatisfy(i -> assertThat(i.message()).doesNotContain(encrypted.password()));
    }

    /** A link to a table that is itself a link in its back-end isn't followed. */
    @Test
    void aLinkToALinkIsNotFollowed() throws IOException {
        Files.copy(CorpusFile.get("jackcess/V2007/linkedV2007.accdb").file(), dir.resolve("linkedV2007.accdb"));
        Path front = dir.resolve("chain.accdb");
        try (Database db = create(front)) {
            db.createLinkedTable("Chained", "C:\\data\\linkedV2007.accdb", "Table2");
            db.createLinkedTable("Direct", "C:\\data\\linkedV2007.accdb", "Table1");
        }
        Extraction extraction = extract(front, resolve(null));
        assertThat(extraction.table("Direct").isResolvedLink()).isTrue();
        assertThat(extraction.table("Chained").isLinked()).isTrue();
        assertThat(extraction.issues(IssueCode.LINKED_TABLE_SKIPPED))
                .singleElement()
                .satisfies(i -> assertThat(i.message()).contains("links of links are not followed"));
    }

    /**
     * A back-end relationship whose name the front-end already uses is renamed; one whose table is linked twice is
     * left out, since which of the two it constrains is unclear.
     */
    @Test
    void backEndRelationshipsAreRenamedOnAClashAndSkippedWhenATableIsLinkedTwice() throws IOException {
        Path back = dir.resolve("back.accdb");
        try (Database db = create(back)) {
            keyed(db, "Parent");
            keyed(db, "Child", "ParentID");
            new RelationshipBuilder("Parent", "Child")
                    .addColumns("ID", "ParentID")
                    .setReferentialIntegrity()
                    .setName("Rel")
                    .toRelationship(db);
        }
        Path front = dir.resolve("front.accdb");
        try (Database db = create(front)) {
            keyed(db, "X");
            keyed(db, "Y", "XID");
            new RelationshipBuilder("X", "Y")
                    .addColumns("ID", "XID")
                    .setReferentialIntegrity()
                    .setName("Rel")
                    .toRelationship(db);
            db.createLinkedTable("P", "D:\\old\\back.accdb", "Parent");
            db.createLinkedTable("C", "D:\\old\\back.accdb", "Child");
        }
        Extraction extraction = extract(front, resolve(null));
        assertThat(extraction.relationship("Rel").childTable()).isEqualTo("Y");
        ForeignKeyModel renamed = extraction.relationship("Rel_2");
        assertThat(renamed.parentTable()).isEqualTo("P");
        assertThat(renamed.childTable()).isEqualTo("C");
        assertThat(renamed.status()).isEqualTo(Status.EMIT);
        assertThat(extraction.issues(IssueCode.IDENTIFIER_RENAMED))
                .singleElement()
                .satisfies(i -> assertThat(i.message()).contains("Rel", "Rel_2", "back.accdb"));

        try (Database db = new DatabaseBuilder(front).open()) {
            db.createLinkedTable("C2", "D:\\old\\back.accdb", "Child");
        }
        Extraction twice = extract(front, resolve(null));
        assertThat(twice.model().relationships())
                .extracting(ForeignKeyModel::name)
                .containsExactly("Rel");
        assertThat(twice.issues(IssueCode.RELATIONSHIP_MALFORMED))
                .singleElement()
                .satisfies(i -> assertThat(i.message()).contains("linked here more than once (C, C2)"));
    }

    /**
     * The back-end's MSysRelationships is data this database doesn't control: a row without its child table (a
     * damaged catalog, written here by hand) is reported as malformed, and the back-end's other relationships still
     * come along.
     */
    @Test
    void aMalformedBackEndRelationshipIsReportedNotACrash() throws IOException {
        Path back = dir.resolve("back.accdb");
        try (Database db = create(back)) {
            keyed(db, "Parent");
            keyed(db, "Child", "ParentID");
            new RelationshipBuilder("Parent", "Child")
                    .addColumns("ID", "ParentID")
                    .setReferentialIntegrity()
                    .setName("Rel")
                    .toRelationship(db);
            Map<String, Object> broken = new HashMap<>();
            broken.put("szRelationship", "Broken");
            broken.put("szReferencedObject", "Parent");
            broken.put("szReferencedColumn", "ID");
            broken.put("szObject", null);
            broken.put("szColumn", "ParentID");
            broken.put("icolumn", 0);
            broken.put("ccolumn", 1);
            broken.put("grbit", 0);
            db.getSystemTable("MSysRelationships").addRowFromMap(broken);
        }
        Path front = dir.resolve("front.accdb");
        try (Database db = create(front)) {
            db.createLinkedTable("P", "D:\\old\\back.accdb", "Parent");
            db.createLinkedTable("C", "D:\\old\\back.accdb", "Child");
        }

        Extraction extraction = extract(front, resolve(null));
        assertThat(extraction.model().relationships())
                .extracting(ForeignKeyModel::name)
                .containsExactly("Rel");
        assertThat(extraction.relationship("Rel").status()).isEqualTo(Status.EMIT);
        assertThat(extraction.issues(IssueCode.RELATIONSHIP_MALFORMED))
                .singleElement()
                .satisfies(i -> {
                    assertThat(i.table()).isEqualTo("P");
                    assertThat(i.object()).isEqualTo("Broken");
                    assertThat(i.message()).contains("back.accdb", "missing a table");
                });
    }

    /** Only the root is searched, and a file name that matches more than one file there ignoring case is an error. */
    @Test
    void severalFilesMatchingIgnoringCaseAreAnError() throws IOException {
        Path root = Files.createDirectory(dir.resolve("root"));
        Files.copy(CorpusFile.get("jackcess/linkeeTest.accdb").file(), root.resolve("Linkee.accdb"));
        try {
            Files.copy(CorpusFile.get("jackcess/linkeeTest.accdb").file(), root.resolve("LINKEE.accdb"));
        } catch (FileAlreadyExistsException e) {
            assumeTrue(false, "the file system ignores case, so it can't hold both names");
        }
        Path front = dir.resolve("front.accdb");
        try (Database db = create(front)) {
            db.createLinkedTable("T", "C:\\x\\linkee.accdb", "Table1");
        }
        assertThatThrownBy(() -> extract(front, resolve(root)))
                .hasMessageContaining("several files in " + root + " match linkee.accdb ignoring case")
                .hasMessageContaining("LINKEE.accdb, Linkee.accdb");
    }

    // ---------------------------------------------------------------- helpers

    private static Database create(Path file) throws IOException {
        return new DatabaseBuilder(file)
                .setFileFormat(Database.FileFormat.V2010)
                .create();
    }

    /** A table with a Long primary key ID and Long columns. */
    private static void keyed(Database db, String name, String... columns) throws IOException {
        TableBuilder table = new TableBuilder(name)
                .addColumn(new ColumnBuilder("ID", DataType.LONG))
                .addIndex(new IndexBuilder(IndexBuilder.PRIMARY_KEY_NAME)
                        .addColumns("ID")
                        .setPrimaryKey());
        for (String column : columns) {
            table.addColumn(new ColumnBuilder(column, DataType.LONG));
        }
        table.toTable(db);
    }

    private static List<Object[]> rows(Path front, TableModel table) throws IOException {
        List<Object[]> rows = new ArrayList<>();
        try (AccessSource source = AccessSource.open(front, resolve(null), new Issues())) {
            SchemaExtractor.extract(source, ExtractOptions.ALL, new Issues());
            source.rows(table).forEachRemaining(rows::add);
        }
        rows.sort(Comparator.comparingLong(r -> ((Number) r[0]).longValue()));
        return rows;
    }

    /** The dumper prints Greek text as is, Currency with a decimal comma, and NULL as {@code <NULL>}. */
    private static void assertValue(Object value, String cell, AccessType type, String where) {
        String expected = GuestDump.value(cell);
        if (expected == null) {
            assertThat(value).as(where).isNull();
        } else if (type == AccessType.MONEY) {
            assertThat((BigDecimal) value).as(where).isEqualByComparingTo(new BigDecimal(expected.replace(',', '.')));
        } else {
            assertThat(String.valueOf(value)).as(where).isEqualTo(expected);
        }
    }
}
