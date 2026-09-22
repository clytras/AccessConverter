package com.lytrax.accessconverter.regression;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.healthmarketscience.jackcess.Database;
import com.lytrax.accessconverter.Extraction;
import com.lytrax.accessconverter.fixtures.Fixtures;
import com.lytrax.accessconverter.fixtures.JackcessCorpus;
import com.lytrax.accessconverter.model.ForeignKeyModel;
import com.lytrax.accessconverter.model.TableModel;
import com.lytrax.accessconverter.report.IssueCode;
import com.lytrax.accessconverter.source.AccessSource;
import java.io.IOException;
import java.nio.file.AccessDeniedException;
import org.junit.jupiter.api.Test;

/**
 * F-14: a linked table broke v2. Opening it failed, and so did {@code db.getRelationships()} as soon as a
 * relationship touched it, which left a half-written dump. v3 never opens it: it's in the model as linked, with
 * its stored target, and reported.
 */
class LinkedTablesSkippedTest {

    @Test
    void theLinkedTableIsInTheModelButNotRead() {
        Extraction extraction = Extraction.of(JackcessCorpus.LINKED_V2007.path());

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
        Extraction extraction = Extraction.of(JackcessCorpus.LINKED_V2007.path());
        assertThat(extraction.relationship("Table1Table2")).satisfies(r -> {
            assertThat(r.parentTable()).isEqualTo("Table1");
            assertThat(r.childTable()).isEqualTo("Table2");
            assertThat(r.status()).isEqualTo(ForeignKeyModel.Status.SKIPPED_LINKED_TABLE);
        });
        assertThat(extraction.issues(IssueCode.FK_SKIPPED_LINKED_TABLE)).hasSize(1);
    }

    @Test
    void theLocalTableStillStreams() throws IOException {
        Extraction extraction = Extraction.of(JackcessCorpus.LINKED_V2007.path());
        try (AccessSource source = AccessSource.open(JackcessCorpus.LINKED_V2007.path())) {
            assertThat(source.rows(extraction.table("Table1"))).toIterable().hasSize(1);
        }
    }

    /**
     * Why the extractor decodes MSysRelationships itself: Jackcess opens the linked table to build the relationship,
     * and Jackcess 5 refuses to resolve linked databases unless a LinkResolver is configured.
     */
    @Test
    void jackcessGetRelationshipsFailsOnThisFile() throws IOException {
        try (Database db = Fixtures.openReadOnly(JackcessCorpus.LINKED_V2007.path())) {
            assertThatThrownBy(db::getRelationships)
                    .isInstanceOf(AccessDeniedException.class)
                    .hasMessageContaining("Linked database resolution is disabled");
        }
    }
}
