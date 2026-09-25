package io.lytrax.accessconverter.profile;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.ColumnBuilder;
import com.healthmarketscience.jackcess.DataType;
import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.DatabaseBuilder;
import com.healthmarketscience.jackcess.IndexBuilder;
import com.healthmarketscience.jackcess.RelationshipBuilder;
import com.healthmarketscience.jackcess.Row;
import com.healthmarketscience.jackcess.Table;
import com.healthmarketscience.jackcess.TableBuilder;
import com.healthmarketscience.jackcess.impl.ColumnImpl;
import com.healthmarketscience.jackcess.impl.DatabaseImpl;
import io.lytrax.accessconverter.Extraction;
import io.lytrax.accessconverter.extract.RelationshipDecoder;
import io.lytrax.accessconverter.model.ForeignKeyModel;
import io.lytrax.accessconverter.model.TableModel;
import io.lytrax.accessconverter.report.IssueCode;
import io.lytrax.accessconverter.report.Issues;
import io.lytrax.accessconverter.source.AccessSource;
import io.lytrax.accessconverter.source.OpenOptions;
import io.lytrax.accessconverter.source.RowStream;
import java.io.IOException;
import java.nio.file.Path;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Orphan checks on a parent whose key index uses a text sort order Jackcess has no index codes for (Japanese here):
 * Jackcess can't seek it, so the parent keys are scanned into a hash set instead of failing the profile. Such
 * files come from Access installations with another database sort order.
 */
class ParentKeysFallbackTest {
    /** Japanese (LCID 0x0411): readable, but Jackcess can't encode index keys for it. */
    private static final ColumnImpl.SortOrder JAPANESE = new ColumnImpl.SortOrder((short) 0x0411, (short) 1);

    @TempDir
    static Path dir;

    static Path file;

    @BeforeAll
    static void createAParentWithAJapaneseKey() throws IOException {
        file = dir.resolve("japanese.accdb");
        try (Database db = new DatabaseBuilder(file)
                .setFileFormat(Database.FileFormat.V2010)
                .create()) {
            db.setEvaluateExpressions(false);
            // Test fixture only: lets Jackcess add rows to a table whose index it can't encode keys for
            ((DatabaseImpl) db).setWriteBrokenIndex(true);
            ColumnBuilder code = new ColumnBuilder("Code", DataType.TEXT).setLengthInUnits(10);
            code.setTextSortOrder(JAPANESE);
            Table parent = new TableBuilder("Parent")
                    .addColumn(code)
                    .addIndex(new IndexBuilder(IndexBuilder.PRIMARY_KEY_NAME)
                            .addColumns("Code")
                            .setPrimaryKey())
                    .toTable(db);
            Table child = new TableBuilder("Child")
                    .addColumn(new ColumnBuilder("ID", DataType.LONG).setAutoNumber(true))
                    .addColumn(new ColumnBuilder("ParentCode", DataType.TEXT).setLengthInUnits(10))
                    .addIndex(new IndexBuilder(IndexBuilder.PRIMARY_KEY_NAME)
                            .addColumns("ID")
                            .setPrimaryKey())
                    .toTable(db);
            // Jackcess can't create an enforced relationship over an index it can't search: create it unenforced,
            // then set the flag in MSysRelationships, where Access keeps it
            new RelationshipBuilder("Parent", "Child")
                    .addColumns("Code", "ParentCode")
                    .toRelationship(db);
            Table relationships = db.getSystemTable("MSysRelationships");
            for (Row row : relationships) {
                row.put("grbit", (Integer) row.get("grbit") & ~RelationshipDecoder.NO_REFERENTIAL_INTEGRITY);
                relationships.updateRow(row);
            }
            parent.addRow("ABC");
            parent.addRow("DEF");
            child.addRow(Column.AUTO_NUMBER, "abc");
            child.addRow(Column.AUTO_NUMBER, "DEF");
            child.addRow(Column.AUTO_NUMBER, "XYZ");
        }
    }

    @Test
    void theFixtureIsAnEnforcedRelationshipJackcessCantSearch() throws IOException {
        Extraction extraction = Extraction.of(file);
        assertThat(extraction.relationship("ParentChild").status()).isEqualTo(ForeignKeyModel.Status.EMIT);
        TableModel parent = extraction.table("Parent");
        try (AccessSource source = AccessSource.open(file)) {
            assertThatThrownBy(() -> source.keyLookup(parent, parent.primaryKey()))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("unsupported collating sort order");
        }
    }

    @Test
    void rowsComeInPhysicalOrderWhenThePrimaryKeyCantBeRead() throws IOException {
        TableModel parent = Extraction.of(file).table("Parent");
        Issues issues = new Issues();
        try (AccessSource source = AccessSource.open(file, OpenOptions.DEFAULT, issues)) {
            RowStream rows = source.rows(parent);
            assertThat(rows.next()).containsExactly("ABC");
            assertThat(rows.next()).containsExactly("DEF");
            assertThat(rows.hasNext()).isFalse();
        }
        assertThat(issues.list()).singleElement().satisfies(i -> {
            assertThat(i.code()).isEqualTo(IssueCode.ROW_ORDER_PHYSICAL);
            assertThat(i.message())
                    .isEqualTo("rows in physical order: the primary-key index can't be read (IllegalArgumentException:"
                            + " unsupported collating sort order SortOrder[0x01000411, Japanese] for text index"
                            + " (Db=japanese.accdb;Table=Parent;Index=0))");
        });
    }

    @Test
    void orphansAndInexactMatchesAreStillCounted() throws IOException {
        Extraction extraction = Extraction.of(file);
        DataProfile profile;
        try (AccessSource source = AccessSource.open(file)) {
            profile = DataProfiler.profile(source, extraction.model());
        }
        assertThat(profile.relationship("ParentChild")).hasValueSatisfying(r -> {
            assertThat(r.scanFallback())
                    .startsWith("IllegalArgumentException: unsupported collating sort order")
                    .doesNotContain("\n");
            assertThat(r.checked()).isEqualTo(3);
            assertThat(r.orphans()).isEqualTo(1);
            assertThat(r.orphanSamples()).containsExactly("(3) -> (\"XYZ\")");
            assertThat(r.inexact()).isEqualTo(1);
            assertThat(r.inexactSamples()).containsExactly("(1): (\"abc\") matches (\"ABC\")");
        });
    }
}
