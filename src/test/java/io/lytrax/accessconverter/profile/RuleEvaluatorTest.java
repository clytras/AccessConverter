package io.lytrax.accessconverter.profile;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.lytrax.accessconverter.extract.ExpressionTranslator;
import io.lytrax.accessconverter.profile.RuleEvaluator.EvaluationException;
import io.lytrax.accessconverter.profile.RuleEvaluator.Truth;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class RuleEvaluatorTest {
    private final RuleEvaluator evaluator = new RuleEvaluator();

    @ParameterizedTest
    @CsvSource(delimiter = '|', quoteCharacter = '~', textBlock = """
            >0                      | 5    | TRUE
            >0                      | 0    | FALSE
            >0                      |      | UNKNOWN
            Is Null                 |      | TRUE
            Is Not Null And >0      |      | FALSE
            Between 1 And 10        | 10   | TRUE
            Not Between 1 And 10    | 11   | TRUE
            In (1, 2)               | 3    | FALSE
            Not In (1, 2)           | 3    | TRUE
            >0 Or Is Null           |      | TRUE
            Not >5                  |      | UNKNOWN
            """)
    void threeValuedLogic(String rule, Integer value, Truth expected) {
        assertThat(test(rule, value)).isEqualTo(expected);
    }

    @Test
    void textComparesLikeAccess() {
        // Case-insensitive, accent-sensitive, trailing spaces ignored, ß = ss (01, unique-index semantics)
        assertThat(test("=\"ABC\"", "abc")).isEqualTo(Truth.TRUE);
        assertThat(test("=\"abc\"", "abc  ")).isEqualTo(Truth.TRUE);
        assertThat(test("=\"resume\"", "résumé")).isEqualTo(Truth.FALSE);
        assertThat(test("=\"strasse\"", "straße")).isEqualTo(Truth.TRUE);
        assertThat(test("<>\"\"", "")).isEqualTo(Truth.FALSE);
        assertThat(test("Like \"a*\"", "Apple")).isEqualTo(Truth.TRUE);
        assertThat(test("Like \"#?\"", "a1")).isEqualTo(Truth.FALSE);
    }

    @Test
    void numbersCompareExactlyAcrossTypes() {
        assertThat(test(">0.1", 0.1f)).isEqualTo(Truth.FALSE);
        assertThat(test("=12.3456", new BigDecimal("12.34560"))).isEqualTo(Truth.TRUE);
        assertThat(test("<=255", 255)).isEqualTo(Truth.TRUE);
        // Access Yes/No is -1/0
        assertThat(test("=-1", true)).isEqualTo(Truth.TRUE);
        assertThat(test("=True", true)).isEqualTo(Truth.TRUE);
    }

    @Test
    void datesCompareAsDates() {
        assertThat(test(">=#1/1/1900#", LocalDateTime.of(1899, 12, 30, 10, 30))).isEqualTo(Truth.FALSE);
        assertThat(test(">=#1/1/1900#", LocalDateTime.of(2024, 1, 2, 3, 4, 5))).isEqualTo(Truth.TRUE);
    }

    @Test
    void tableRulesReadSeveralColumns() {
        Map<String, Object> row = new HashMap<>();
        row.put("StartDate", LocalDateTime.of(2024, 1, 2, 0, 0));
        row.put("EndDate", LocalDateTime.of(2024, 1, 1, 0, 0));
        var rule = ExpressionTranslator.translateTableRule("[EndDate]>=[StartDate]", null)
                .expr();
        assertThat(evaluator.test(rule, row::get)).isEqualTo(Truth.FALSE);
    }

    @Test
    void incompatibleTypesAreNotGuessed() {
        assertThatThrownBy(() -> test(">0", "text"))
                .isInstanceOf(EvaluationException.class)
                .hasMessageContaining("cannot compare text with a number");
        assertThatThrownBy(() -> test(">0", Double.NaN)).isInstanceOf(EvaluationException.class);
    }

    private Truth test(String rule, Object value) {
        var expr = ExpressionTranslator.translateColumnRule(rule, null, "v").expr();
        assertThat(expr).as("translated: " + rule).isNotNull();
        Map<String, Object> row = new HashMap<>();
        row.put("v", value);
        return evaluator.test(expr, row::get);
    }
}
