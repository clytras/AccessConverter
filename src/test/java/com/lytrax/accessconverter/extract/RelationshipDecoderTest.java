package com.lytrax.accessconverter.extract;

import static org.assertj.core.api.Assertions.assertThat;

import com.lytrax.accessconverter.extract.RelationshipDecoder.Decoded;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Decoding {@code MSysRelationships} rows; the cross-check against Jackcess is RelationshipFlagsTest. */
class RelationshipDecoderTest {

    @Test
    void groupsRowsByNameAndOrdersColumnsByPosition() {
        List<Decoded> decoded = RelationshipDecoder.decode(List.of(
                row("OrderLines", "Orders", "B", "Lines", "OrderB", 1, 2, 0),
                row("orderlines", "Orders", "A", "Lines", "OrderA", 0, 2, 0),
                row("Another", "P", "id", "C", "pid", 0, 1, 0)));
        assertThat(decoded).extracting(Decoded::name).containsExactly("Another", "OrderLines");
        Decoded lines = decoded.get(1);
        assertThat(lines.parentTable()).isEqualTo("Orders");
        assertThat(lines.childTable()).isEqualTo("Lines");
        assertThat(lines.parentColumns()).containsExactly("A", "B");
        assertThat(lines.childColumns()).containsExactly("OrderA", "OrderB");
        assertThat(lines.problem()).isNull();
    }

    @Test
    void keepsTheFlags() {
        int flags = RelationshipDecoder.CASCADE_UPDATES
                | RelationshipDecoder.CASCADE_NULL
                | RelationshipDecoder.LEFT_OUTER_JOIN;
        Decoded d = RelationshipDecoder.decode(List.of(row("R", "P", "id", "C", "pid", 0, 1, flags)))
                .getFirst();
        assertThat(d.has(RelationshipDecoder.CASCADE_UPDATES)).isTrue();
        assertThat(d.has(RelationshipDecoder.CASCADE_NULL)).isTrue();
        assertThat(d.has(RelationshipDecoder.CASCADE_DELETES)).isFalse();
        assertThat(d.has(RelationshipDecoder.NO_REFERENTIAL_INTEGRITY)).isFalse();
    }

    @Test
    void inconsistentRowsAreReportedNotGuessed() {
        assertThat(problem(row("R", "P", "a", "C", "x", 0, 2, 0))).contains("expected column positions 0..1");
        assertThat(problem(row("R", "P", "a", "C", "x", 0, 2, 0), row("R", "P", "b", "C", "y", 0, 2, 0)))
                .contains("appears twice");
        assertThat(problem(row("R", "P", "a", "C", "x", 0, 2, 0), row("R", "Q", "b", "C", "y", 1, 2, 0)))
                .contains("different tables");
        assertThat(problem(row("R", "P", "a", "C", "x", 0, 2, 0), row("R", "P", "b", "C", "y", 1, 2, 2)))
                .contains("flags");
        Map<String, Object> missing = row("R", "P", "a", "C", "x", 0, 1, 0);
        missing.remove("szColumn");
        assertThat(problem(missing)).contains("missing");
    }

    @SafeVarargs
    private static String problem(Map<String, ?>... rows) {
        return RelationshipDecoder.decode(List.of(rows)).getFirst().problem();
    }

    private static Map<String, Object> row(
            String name,
            String parent,
            String parentColumn,
            String child,
            String childColumn,
            int position,
            int count,
            int flags) {
        Map<String, Object> row = new HashMap<>();
        row.put("szRelationship", name);
        row.put("szReferencedObject", parent);
        row.put("szReferencedColumn", parentColumn);
        row.put("szObject", child);
        row.put("szColumn", childColumn);
        row.put("icolumn", position);
        row.put("ccolumn", count);
        row.put("grbit", flags);
        return row;
    }
}
