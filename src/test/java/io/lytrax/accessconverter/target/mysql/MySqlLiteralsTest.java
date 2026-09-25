package io.lytrax.accessconverter.target.mysql;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.LocalDateTime;
import net.jqwik.api.Assume;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import org.junit.jupiter.api.Test;

/** 05, Value literals: what the dump writes for each canonical value (F-02). */
class MySqlLiteralsTest {

    @Test
    void everyCharacterTheServerOrTheClientReadsSpeciallyIsEscaped() {
        // F-02: v2 only doubled quotes, so C:\temp\new gained a tab and a newline
        assertThat(MySqlLiterals.string("C:\\temp\\new")).isEqualTo("'C:\\\\temp\\\\new'");
        assertThat(MySqlLiterals.string("it's \"quoted\"")).isEqualTo("'it\\'s \\\"quoted\\\"'");
        assertThat(MySqlLiterals.string("a\0b\u001Ac\bd\ne\rf\tg")).isEqualTo("'a\\0b\\Zc\\bd\\ne\\rf\\tg'");
        // FavDatabase's "\/" sequences cost v2 three INSERT batches
        assertThat(MySqlLiterals.string("Lors des \\/D1")).isEqualTo("'Lors des \\\\/D1'");
        assertThat(MySqlLiterals.string("Γιώργος 😀")).isEqualTo("'Γιώργος 😀'");
    }

    @Property
    void aLiteralReadsBackAsTheText(@ForAll String text) {
        Assume.that(!MySqlLiterals.hasUnpairedSurrogate(text));
        assertThat(MySqlVerifier.unescape(MySqlLiterals.string(text))).isEqualTo(text);
    }

    @Test
    void unpairedSurrogatesAreFoundAndReplaced() {
        assertThat(MySqlLiterals.hasUnpairedSurrogate("ok 😀")).isFalse();
        assertThat(MySqlLiterals.hasUnpairedSurrogate("a\uD83Db")).isTrue();
        assertThat(MySqlLiterals.hasUnpairedSurrogate("\uDE00")).isTrue();
        assertThat(MySqlLiterals.replaceUnpairedSurrogates("a\uD83Db😀")).isEqualTo("a\uFFFDb😀");
    }

    @Test
    void aSingleIsWrittenSoTheServerStoresExactlyIt() {
        assertThat(MySqlLiterals.floatValue(2583.2092f)).isEqualTo("2583.2092");
        assertThat(MySqlLiterals.floatValue(1.0E10f)).isEqualTo("1.0E10");
        // As a double, 3.4028235E38 is over the float range and the server rejects it (ERROR 1264, measured)
        assertThat(MySqlLiterals.floatValue(Float.MAX_VALUE)).isEqualTo("3.4028234663852886E38");
        assertThat((float) Double.parseDouble(MySqlLiterals.floatValue(-Float.MAX_VALUE)))
                .isEqualTo(-Float.MAX_VALUE);
        assertThat(MySqlLiterals.doubleValue(0.1)).isEqualTo("0.1");
    }

    @Property
    void everySingleReadsBackAsItself(@ForAll float value) {
        Assume.that(Float.isFinite(value));
        String written = MySqlLiterals.floatValue(value);
        double parsed = Double.parseDouble(written);
        assertThat((float) parsed).isEqualTo(value);
        assertThat(Math.abs(parsed)).isLessThanOrEqualTo(Float.MAX_VALUE);
    }

    @Test
    void datesKeepFourDigitYearsAndRoundAsTheServerDoes() {
        LocalDateTime early = LocalDateTime.of(201, 3, 4, 5, 6, 7);
        assertThat(MySqlLiterals.dateTime(early, 0)).isEqualTo("'0201-03-04 05:06:07'");
        LocalDateTime extended = LocalDateTime.of(2020, 1, 2, 3, 4, 5, 123_456_700);
        assertThat(MySqlLiterals.atPrecision(extended, 6)).isEqualTo(extended.withNano(123_457_000));
        assertThat(MySqlLiterals.losesFraction(extended, 6)).isTrue();
        assertThat(MySqlLiterals.losesFraction(extended.withNano(123_456_000), 6))
                .isFalse();
        // Rounding up past the last DATETIME would overflow it; the value is cut instead
        LocalDateTime last = LocalDateTime.of(9999, 12, 31, 23, 59, 59, 999_999_900);
        assertThat(MySqlLiterals.atPrecision(last, 6)).isEqualTo(last.withNano(999_999_000));
        assertThat(MySqlLiterals.fitsDateTime(LocalDateTime.of(10000, 2, 28, 0, 0)))
                .isFalse();
        assertThat(MySqlLiterals.fitsDateTime(LocalDateTime.of(100, 1, 1, 0, 0)))
                .isTrue();
    }

    @Test
    void bytesAreHexAndAnEmptyValueStaysEmpty() {
        assertThat(MySqlLiterals.bytes(new byte[] {0, (byte) 0xFF, 0x1A})).isEqualTo("X'00FF1A'");
        assertThat(MySqlLiterals.bytes(new byte[0])).isEqualTo("X''");
    }

    @Test
    void identifiersDoubleTheirBackticks() {
        assertThat(MySqlLiterals.identifier("Order Details")).isEqualTo("`Order Details`");
        assertThat(MySqlLiterals.identifier("a`b")).isEqualTo("`a``b`");
    }

    @Test
    void utf8LengthCountsWhatTheFileHolds() {
        String text = "aé€😀";
        assertThat(MySqlLiterals.utf8Length(text))
                .isEqualTo(text.getBytes(java.nio.charset.StandardCharsets.UTF_8).length);
    }
}
