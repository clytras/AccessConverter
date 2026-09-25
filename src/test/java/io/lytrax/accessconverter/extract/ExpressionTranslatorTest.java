package io.lytrax.accessconverter.extract;

import static org.assertj.core.api.Assertions.assertThat;

import io.lytrax.accessconverter.model.CheckRule;
import io.lytrax.accessconverter.model.DefaultValue;
import io.lytrax.accessconverter.model.expr.Expr;
import io.lytrax.accessconverter.model.expr.Expr.BooleanLiteral;
import io.lytrax.accessconverter.model.expr.Expr.ColumnRef;
import io.lytrax.accessconverter.model.expr.Expr.Comparison;
import io.lytrax.accessconverter.model.expr.Expr.CurrentDateTime;
import io.lytrax.accessconverter.model.expr.Expr.DateTimeLiteral;
import io.lytrax.accessconverter.model.expr.Expr.NewGuid;
import io.lytrax.accessconverter.model.expr.Expr.NullLiteral;
import io.lytrax.accessconverter.model.expr.Expr.NumberLiteral;
import io.lytrax.accessconverter.model.expr.Expr.Operator;
import io.lytrax.accessconverter.model.expr.Expr.StringLiteral;
import io.lytrax.accessconverter.model.expr.ExprPrinter;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/** The expression subset of 04, and everything outside it becoming "unsupported" with a reason. */
class ExpressionTranslatorTest {

    @Nested
    class Defaults {
        @ParameterizedTest
        @CsvSource(delimiter = '|', quoteCharacter = '~', textBlock = """
                0                | 0
                =0               | 0
                -1.5             | -1.5
                -1               | -1
                1e3              | 1000
                "Greece"         | "Greece"
                'single'         | "single"
                "say ""hi""\"     | "say ""hi""\"
                ""               | ""
                Yes              | True
                no               | False
                True             | True
                On               | True
                Off              | False
                Null             | Null
                Now()            | Now()
                =Now()           | Now()
                date()           | Date()
                Time()           | Time()
                GenGUID()        | GenGUID()
                #1/31/2000#      | #2000-01-31#
                #2000-01-31 10:00# | #2000-01-31 10:00:00#
                #12/31/1999 11:59:59 PM# | #1999-12-31 23:59:59#
                #10:30 AM#       | #10:30:00#
                =(5)             | 5
                """)
        void translates(String access, String normalized) {
            DefaultValue d = ExpressionTranslator.translateDefault(access);
            assertThat(d.unsupportedReason()).isNull();
            assertThat(ExprPrinter.print(d.expr())).isEqualTo(normalized);
            assertThat(d.raw()).isEqualTo(access);
        }

        @Test
        void literalsHaveTheirTypes() {
            assertThat(expr("-1")).isEqualTo(new NumberLiteral(new BigDecimal("-1")));
            assertThat(expr("Yes")).isEqualTo(new BooleanLiteral(true));
            assertThat(expr("\"Greece\"")).isEqualTo(new StringLiteral("Greece"));
            assertThat(expr("Null")).isEqualTo(new NullLiteral());
            assertThat(expr("=Now()")).isEqualTo(new CurrentDateTime(CurrentDateTime.Part.NOW));
            assertThat(expr("GenGUID()")).isEqualTo(new NewGuid());
            assertThat(expr("#10:30 PM#"))
                    .isEqualTo(new DateTimeLiteral(LocalDateTime.of(1899, 12, 30, 22, 30), false, true));
            assertThat(expr("#12/1/2000#"))
                    .isEqualTo(new DateTimeLiteral(LocalDateTime.of(2000, 12, 1, 0, 0), true, false));
        }

        @ParameterizedTest
        @CsvSource(delimiter = '|', quoteCharacter = '~', textBlock = """
                =Date()+1              | arithmetic
                ="a" & "b"             | arithmetic
                DateAdd("d",1,Date())  | with arguments
                Len("x")               | with arguments
                Nz()                   | function Nz()
                [Other]                | can't refer to a column
                >0                     | left operand
                =1=1                   | single value
                #1/31/00#              | two-digit
                #13/1/2000#            | invalid date
                #yesterday#            | unrecognized date
                "unterminated          | unterminated string
                &H1F                   | unexpected
                """)
        void anythingElseIsUnsupported(String access, String reason) {
            DefaultValue d = ExpressionTranslator.translateDefault(access);
            assertThat(d.isTranslated()).isFalse();
            assertThat(d.unsupportedReason()).contains(reason);
        }

        @Test
        void blankIsUnsupported() {
            assertThat(ExpressionTranslator.translateDefault("  ").unsupportedReason())
                    .isEqualTo("empty expression");
        }

        private Expr expr(String access) {
            return ExpressionTranslator.translateDefault(access).expr();
        }
    }

    @Nested
    class ColumnRules {
        @ParameterizedTest
        @CsvSource(delimiter = '|', quoteCharacter = '~', textBlock = """
                >0                        | [Qty] > 0
                > 0                       | [Qty] > 0
                =5                        | [Qty] = 5
                <>""                      | [Qty] <> ""
                >=0 And <=100             | [Qty] >= 0 And [Qty] <= 100
                <=1 And >=0               | [Qty] <= 1 And [Qty] >= 0
                >=#1/1/1900#              | [Qty] >= #1900-01-01#
                Between 1 And 10          | [Qty] Between 1 And 10
                Not Between 1 And 10      | [Qty] Not Between 1 And 10
                In (1, 2, 3)              | [Qty] In (1, 2, 3)
                Not In ("a","b")          | [Qty] Not In ("a", "b")
                Is Null                   | [Qty] Is Null
                Is Not Null And >0        | [Qty] Is Not Null And [Qty] > 0
                Like "A*"                 | [Qty] Like "A*"
                Not Like "?#"             | [Qty] Not Like "?#"
                Not >5                    | Not [Qty] > 5
                (>0 And <10) Or Is Null   | [Qty] > 0 And [Qty] < 10 Or [Qty] Is Null
                >0 And (<5 Or >10)        | [Qty] > 0 And ([Qty] < 5 Or [Qty] > 10)
                [Qty] > -1                | [Qty] > -1
                """)
        void translates(String access, String normalized) {
            CheckRule r = ExpressionTranslator.translateColumnRule(access, "message", "Qty");
            assertThat(r.unsupportedReason()).isNull();
            assertThat(ExprPrinter.print(r.expr())).isEqualTo(normalized);
            assertThat(r.validationText()).isEqualTo("message");
        }

        @Test
        void theImplicitOperandIsTheColumn() {
            assertThat(ExpressionTranslator.translateColumnRule(">0", null, "Qty")
                            .expr())
                    .isEqualTo(new Comparison(Operator.GT, new ColumnRef("Qty"), new NumberLiteral(BigDecimal.ZERO)));
        }

        @ParameterizedTest
        @CsvSource(delimiter = '|', quoteCharacter = '~', textBlock = """
                <=Date()             | non-deterministic
                >Now()-30            | arithmetic
                =GenGUID()           | non-deterministic
                >0 Xor <5            | Xor
                Like "[A-Z]*"        | character classes
                Like 5               | string pattern
                [a]+[b]>0            | arithmetic
                Len([Qty])>0         | with arguments
                5                    | must be a condition
                Yes                  | must be a condition
                [a] Not = 5          | unexpected 'Not'
                [t].[c] > 0          | qualified
                1 < [Qty] < 5        | chained
                > 0 )                | unexpected ')'
                Between 1            | expected 'and'
                """)
        void anythingElseIsUnsupported(String access, String reason) {
            CheckRule r = ExpressionTranslator.translateColumnRule(access, null, "Qty");
            assertThat(r.isTranslated()).isFalse();
            assertThat(r.unsupportedReason()).contains(reason);
        }
    }

    @Nested
    class TableRules {
        @ParameterizedTest
        @CsvSource(delimiter = '|', quoteCharacter = '~', textBlock = """
                [EndDate]>=[StartDate]                     | [EndDate] >= [StartDate]
                EndDate >= StartDate                       | [EndDate] >= [StartDate]
                [Discount] Is Null Or [Discount]<=[Price]  | [Discount] Is Null Or [Discount] <= [Price]
                [Τιμή] > 0                                 | [Τιμή] > 0
                """)
        void translates(String access, String normalized) {
            assertThat(ExprPrinter.print(ExpressionTranslator.translateTableRule(access, null)
                            .expr()))
                    .isEqualTo(normalized);
        }

        @Test
        void anImplicitOperandNeedsAColumn() {
            assertThat(ExpressionTranslator.translateTableRule(">0", null).unsupportedReason())
                    .contains("left operand");
        }
    }
}
