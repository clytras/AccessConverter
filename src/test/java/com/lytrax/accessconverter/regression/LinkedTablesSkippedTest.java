package com.lytrax.accessconverter.regression;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.healthmarketscience.jackcess.Database;
import com.lytrax.accessconverter.Extraction;
import com.lytrax.accessconverter.fixtures.CorpusFile;
import com.lytrax.accessconverter.fixtures.Fixtures;
import com.lytrax.accessconverter.model.ForeignKeyModel;
import com.lytrax.accessconverter.model.TableModel;
import com.lytrax.accessconverter.report.IssueCode;
import com.lytrax.accessconverter.source.AccessSource;
import com.lytrax.accessconverter.target.json.JsonFixture;
import com.lytrax.accessconverter.target.json.JsonOptions;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AccessDeniedException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * F-14: a linked table broke v2. Opening it failed, and so did {@code db.getRelationships()} as soon as a
 * relationship touched it, which left a half-written dump. v3 never opens it: it's in the model as linked, with
 * its stored target, and reported.
 */
class LinkedTablesSkippedTest {

    @TempDir
    Path dir;

    /**
     * The JSON export keeps a linked table's connection string as Access stores it, so an ODBC password goes into
     * the file: the report says so, naming the table, and nothing is said when the schema is left out or the link
     * is to a file.
     */
    @Test
    void aJsonExportThatCarriesAnOdbcPasswordSaysSo() throws IOException {
        Path odbc = CorpusFile.get("jackcess/V2007/linkedOdbcV2007.accdb").file();
        var converted = JsonFixture.convert(odbc, dir.resolve("odbc.json"));
        assertThat(converted.issues(IssueCode.LINKED_CONNECTION_PASSWORD))
                .singleElement()
                .satisfies(i -> {
                    assertThat(i.table()).isEqualTo("Ordrar");
                    assertThat(i.message()).contains("password included", "--no-schema");
                });
        assertThat(Files.readString(converted.output(), StandardCharsets.UTF_8)).contains("PWD=DummyPassword");

        JsonOptions noSchema = new JsonOptions(
                JsonOptions.Layout.DOCUMENT,
                JsonOptions.Rows.OBJECT,
                JsonOptions.NumberForm.NUMBER,
                JsonOptions.NumberForm.NUMBER,
                JsonOptions.Hyperlinks.STRING,
                false,
                false);
        assertThat(JsonFixture.convert(odbc, dir.resolve("noschema.json"), noSchema)
                        .issues(IssueCode.LINKED_CONNECTION_PASSWORD))
                .isEmpty();
        assertThat(JsonFixture.convert(
                                CorpusFile.get("jackcess/V2007/linkedV2007.accdb")
                                        .file(),
                                dir.resolve("file.json"))
                        .issues(IssueCode.LINKED_CONNECTION_PASSWORD))
                .isEmpty();
    }

    @Test
    void theLinkedTableIsInTheModelButNotRead() {
        Extraction extraction =
                Extraction.of(CorpusFile.get("jackcess/V2007/linkedV2007.accdb").file());

        TableModel local = extraction.table("Table1");
        assertThat(local.isLinked()).isFalse();
        assertThat(local.columns()).extracting(c -> c.name()).containsExactly("ID", "Field1");

        TableModel linked = extraction.table("Table2");
        assertThat(linked.isLinked()).isTrue();
        assertThat(linked.link().database()).isEqualTo("Z:\\jackcess_test\\linkeeTest.accdb");
        assertThat(linked.link().remoteTable()).isEqualTo("Table1");
        assertThat(linked.columns()).isEmpty();
        assertThat(extraction.issues(IssueCode.LINKED_TABLE_SKIPPED))
                .singleElement()
                .satisfies(i -> assertThat(i.table()).isEqualTo("Table2"));
    }

    @Test
    void relationshipsToItAreDecodedAndSkipped() {
        Extraction extraction =
                Extraction.of(CorpusFile.get("jackcess/V2007/linkedV2007.accdb").file());
        assertThat(extraction.relationship("Table1Table2")).satisfies(r -> {
            assertThat(r.parentTable()).isEqualTo("Table1");
            assertThat(r.childTable()).isEqualTo("Table2");
            assertThat(r.status()).isEqualTo(ForeignKeyModel.Status.SKIPPED_LINKED_TABLE);
        });
        assertThat(extraction.issues(IssueCode.FK_SKIPPED_LINKED_TABLE)).hasSize(1);
    }

    @Test
    void theLocalTableStillStreams() throws IOException {
        Extraction extraction =
                Extraction.of(CorpusFile.get("jackcess/V2007/linkedV2007.accdb").file());
        try (AccessSource source = AccessSource.open(
                CorpusFile.get("jackcess/V2007/linkedV2007.accdb").file())) {
            assertThat(source.rows(extraction.table("Table1"))).toIterable().hasSize(1);
        }
    }

    /**
     * Why the extractor decodes MSysRelationships itself: Jackcess opens the linked table to build the relationship,
     * and Jackcess 5 refuses to resolve linked databases unless a LinkResolver is configured.
     */
    @Test
    void jackcessGetRelationshipsFailsOnThisFile() throws IOException {
        try (Database db = Fixtures.openReadOnly(
                CorpusFile.get("jackcess/V2007/linkedV2007.accdb").file())) {
            assertThatThrownBy(db::getRelationships)
                    .isInstanceOf(AccessDeniedException.class)
                    .hasMessageContaining("Linked database resolution is disabled");
        }
    }
}
