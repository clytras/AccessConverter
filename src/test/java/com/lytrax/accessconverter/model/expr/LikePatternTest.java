package com.lytrax.accessconverter.model.expr;

import static org.assertj.core.api.Assertions.assertThat;

import com.lytrax.accessconverter.model.expr.LikePattern.AnyChar;
import com.lytrax.accessconverter.model.expr.LikePattern.AnyDigit;
import com.lytrax.accessconverter.model.expr.LikePattern.AnyString;
import com.lytrax.accessconverter.model.expr.LikePattern.Literal;
import org.junit.jupiter.api.Test;

class LikePatternTest {

    @Test
    void parsesAccessWildcards() {
        LikePattern p = LikePattern.parse("A*b?#").orElseThrow();
        assertThat(p.elements())
                .containsExactly(new Literal("A"), new AnyString(), new Literal("b"), new AnyChar(), new AnyDigit());
        assertThat(p.mapsToSqlLike()).isFalse();
        assertThat(LikePattern.parse("A*").orElseThrow().mapsToSqlLike()).isTrue();
    }

    @Test
    void characterClassesAreUnsupported() {
        assertThat(LikePattern.parse("[A-Z]*")).isEmpty();
    }

    @Test
    void matchesLikeAccess() {
        assertThat(LikePattern.parse("a*")
                        .orElseThrow()
                        .toRegex()
                        .matcher("ABC")
                        .matches())
                .isTrue();
        assertThat(LikePattern.parse("#?").orElseThrow().toRegex().matcher("7x").matches())
                .isTrue();
        assertThat(LikePattern.parse("#?").orElseThrow().toRegex().matcher("xx").matches())
                .isFalse();
        // Regex metacharacters in a literal are literal
        assertThat(LikePattern.parse("a.b")
                        .orElseThrow()
                        .toRegex()
                        .matcher("aXb")
                        .matches())
                .isFalse();
    }
}
