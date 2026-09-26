package io.lytrax.accessconverter.regression;

import static org.assertj.core.api.Assertions.assertThat;

import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.Relationship;
import io.lytrax.accessconverter.Extraction;
import io.lytrax.accessconverter.fixtures.CorpusCase;
import io.lytrax.accessconverter.fixtures.Fixtures;
import io.lytrax.accessconverter.fixtures.LocalSample;
import io.lytrax.accessconverter.fixtures.LocalSamples;
import io.lytrax.accessconverter.model.ForeignKeyModel;
import io.lytrax.accessconverter.model.ForeignKeyModel.Action;
import io.lytrax.accessconverter.model.ForeignKeyModel.Join;
import io.lytrax.accessconverter.source.OpenOptions;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * The MSysRelationships decoder (04, Relationships) cross-checked against {@code db.getRelationships()} on every
 * file where Jackcess can resolve all relationships, i.e. every file without a linked table: names, tables,
 * column pairs in order, and every flag.
 */
class RelationshipFlagsTest {

    /** One relationship, as both sides describe it. */
    record Flags(
            String name,
            String parent,
            List<String> parentColumns,
            String child,
            List<String> childColumns,
            boolean enforced,
            boolean cascadeUpdates,
            boolean cascadeDeletes,
            boolean cascadeNull,
            boolean oneToOne,
            boolean leftOuter,
            boolean rightOuter) {

        static Flags of(Relationship r) {
            return new Flags(
                    r.getName(),
                    r.getFromTable().getName(),
                    r.getFromColumns().stream().map(Column::getName).toList(),
                    r.getToTable().getName(),
                    r.getToColumns().stream().map(Column::getName).toList(),
                    r.hasReferentialIntegrity(),
                    r.cascadeUpdates(),
                    r.cascadeDeletes(),
                    r.cascadeNullOnDelete(),
                    r.isOneToOne(),
                    r.isLeftOuterJoin(),
                    r.isRightOuterJoin());
        }

        static Flags of(ForeignKeyModel r) {
            return new Flags(
                    r.name(),
                    r.parentTable(),
                    r.parentColumns(),
                    r.childTable(),
                    r.childColumns(),
                    r.enforced(),
                    r.onUpdate() == Action.CASCADE,
                    r.onDelete() == Action.CASCADE,
                    r.onDelete() == Action.SET_NULL,
                    r.oneToOne(),
                    r.join() == Join.LEFT_OUTER,
                    r.join() == Join.RIGHT_OUTER);
        }
    }

    /** Jackcess can't build the relationship to a linked table (its target isn't there): see LinkedTablesSkipped. */
    static Stream<CorpusCase> withoutLinkedTables() {
        return CorpusCase.databases()
                .filter(c -> !c.id().equals("jackcess/V2007/linkedV2007.accdb")
                        && !c.id().equals("access97/linkFront97.mdb"));
    }

    @ParameterizedTest
    @MethodSource("withoutLinkedTables")
    void decoderAgreesWithJackcess(CorpusCase database) throws IOException {
        assertAgrees(database.file(), database.options());
    }

    static void assertAgrees(Path file, OpenOptions options) throws IOException {
        Extraction extraction = Extraction.of(file, options);
        List<Flags> jackcess;
        // Names in the charset the converter decodes with: an Access 97 file's are in its code page
        try (Database db = Fixtures.openReadOnly(
                file,
                options.password(),
                Charset.forName(extraction.model().source().charset()))) {
            jackcess = db.getRelationships().stream().map(Flags::of).toList();
        }
        List<Flags> decoded =
                extraction.model().relationships().stream().map(Flags::of).toList();
        assertThat(decoded).containsExactlyInAnyOrderElementsOf(jackcess);
    }

    @Nested
    @LocalSamples
    class Samples {
        @ParameterizedTest
        @EnumSource(LocalSample.class)
        void decoderAgreesWithJackcess(LocalSample sample) throws IOException {
            assertAgrees(sample.path(), OpenOptions.DEFAULT);
        }
    }
}
