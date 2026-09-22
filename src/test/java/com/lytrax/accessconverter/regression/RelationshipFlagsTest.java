package com.lytrax.accessconverter.regression;

import static org.assertj.core.api.Assertions.assertThat;

import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.Relationship;
import com.lytrax.accessconverter.Extraction;
import com.lytrax.accessconverter.fixtures.Fixtures;
import com.lytrax.accessconverter.fixtures.GeneratedFixture;
import com.lytrax.accessconverter.fixtures.JackcessCorpus;
import com.lytrax.accessconverter.fixtures.LocalSample;
import com.lytrax.accessconverter.fixtures.LocalSamples;
import com.lytrax.accessconverter.model.ForeignKeyModel;
import com.lytrax.accessconverter.model.ForeignKeyModel.Action;
import com.lytrax.accessconverter.model.ForeignKeyModel.Join;
import java.io.IOException;
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

    static Stream<Path> withoutLinkedTables() {
        return Stream.concat(
                Stream.of(GeneratedFixture.values()).map(GeneratedFixture::path),
                Stream.of(JackcessCorpus.values())
                        .filter(f -> f != JackcessCorpus.LINKED_V2007)
                        .map(JackcessCorpus::path));
    }

    @ParameterizedTest
    @MethodSource("withoutLinkedTables")
    void decoderAgreesWithJackcess(Path file) throws IOException {
        assertAgrees(file);
    }

    static void assertAgrees(Path file) throws IOException {
        List<Flags> jackcess;
        try (Database db = Fixtures.openReadOnly(file)) {
            jackcess = db.getRelationships().stream().map(Flags::of).toList();
        }
        List<Flags> decoded = Extraction.of(file).model().relationships().stream()
                .map(Flags::of)
                .toList();
        assertThat(decoded).containsExactlyInAnyOrderElementsOf(jackcess);
    }

    @Nested
    @LocalSamples
    class Samples {
        @ParameterizedTest
        @EnumSource(LocalSample.class)
        void decoderAgreesWithJackcess(LocalSample sample) throws IOException {
            assertAgrees(sample.path());
        }
    }
}
