package io.lytrax.accessconverter.verify;

import static org.assertj.core.api.Assertions.assertThat;

import io.lytrax.accessconverter.verify.VerifyResult.Collector;
import io.lytrax.accessconverter.verify.VerifyResult.Difference;
import org.junit.jupiter.api.Test;

/** 09: what a verification keeps of its differences for the report. */
class VerifyResultTest {

    /**
     * A source table that can't be read (04) is named even after the kept differences run out: its rows that could be
     * read before the damage come first, one difference each, and would otherwise hide why the rest are missing.
     */
    @Test
    void anUnreadableSourceTableIsKeptPastTheLimit() {
        Collector differences = new Collector();
        for (int row = 1; row <= VerifyResult.MAX_DIFFERENCES + 5; row++) {
            differences.add("Fav", null, "row " + row, "a row", "no row");
        }
        Difference unreadable = new Difference("Fav", null, "the source table", "readable", "unreadable: damaged");
        differences.keep(unreadable);

        Collector merged = new Collector();
        merged.addAll(differences);
        VerifyResult result = merged.result();

        assertThat(result.differenceCount()).isEqualTo(VerifyResult.MAX_DIFFERENCES + 6);
        assertThat(result.differences())
                .hasSize(VerifyResult.MAX_DIFFERENCES + 1)
                .endsWith(unreadable);
    }
}
