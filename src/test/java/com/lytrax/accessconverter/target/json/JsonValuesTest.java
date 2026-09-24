package com.lytrax.accessconverter.target.json;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.lytrax.accessconverter.target.json.JsonValues.Hyperlink;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.Random;
import org.junit.jupiter.api.Test;
import tools.jackson.core.JsonGenerator;
import tools.jackson.core.JsonParser;
import tools.jackson.core.ObjectWriteContext;

class JsonValuesTest {

    @Test
    void dateTimesHaveFractionsOnlyWhenTheyHaveAny() {
        assertThat(JsonValues.dateTime(LocalDateTime.of(2024, 1, 2, 3, 4, 5))).isEqualTo("2024-01-02T03:04:05");
        assertThat(JsonValues.dateTime(LocalDateTime.of(2024, 1, 2, 3, 4, 5, 10_000_000)))
                .isEqualTo("2024-01-02T03:04:05.010");
        assertThat(JsonValues.dateTime(LocalDateTime.of(2024, 1, 2, 3, 4, 5, 10_000)))
                .isEqualTo("2024-01-02T03:04:05.000010");
        assertThat(JsonValues.dateTime(LocalDateTime.of(2024, 1, 2, 3, 4, 5, 1)))
                .isEqualTo("2024-01-02T03:04:05.000000001");
    }

    @Test
    void yearsKeepFourDigitsAndPastNineThousandNineHundredNinetyNineTakeASign() {
        assertThat(JsonValues.dateTime(LocalDateTime.of(100, 1, 1, 0, 0))).isEqualTo("0100-01-01T00:00:00");
        // Money files use such dates as markers; JSON keeps them where DATETIME can't
        assertThat(JsonValues.dateTime(LocalDateTime.of(10000, 1, 1, 0, 0))).isEqualTo("+10000-01-01T00:00:00");
    }

    @Test
    void extendedDateTimesAlwaysHaveSevenDigits() {
        assertThat(JsonValues.extendedDateTime(LocalDateTime.of(2021, 6, 14, 0, 0)))
                .isEqualTo("2021-06-14T00:00:00.0000000");
        assertThat(JsonValues.extendedDateTime(LocalDateTime.of(2021, 6, 14, 22, 45, 12, 345_678_900)))
                .isEqualTo("2021-06-14T22:45:12.3456789");
        assertThatThrownBy(() -> JsonValues.extendedDateTime(LocalDateTime.of(2021, 6, 14, 0, 0, 0, 1)))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void hyperlinksSplitIntoTheirParts() {
        assertThat(Hyperlink.parse("Google#https://google.com#"))
                .isEqualTo(new Hyperlink("Google", "https://google.com", null, null));
        assertThat(Hyperlink.parse("#https://google.com#"))
                .isEqualTo(new Hyperlink(null, "https://google.com", null, null));
        assertThat(Hyperlink.parse("Doc#a.doc#Sheet1!A1#tip#"))
                .isEqualTo(new Hyperlink("Doc", "a.doc", "Sheet1!A1", "tip"));
        assertThat(Hyperlink.parse("Doc#a.doc##tip # with a hash#"))
                .isEqualTo(new Hyperlink("Doc", "a.doc", null, "tip # with a hash"));
        assertThat(Hyperlink.parse("no hash at all")).isEqualTo(new Hyperlink("no hash at all", null, null, null));
        assertThat(Hyperlink.parse("")).isEqualTo(new Hyperlink(null, null, null, null));
    }

    @Test
    void aLoneSurrogateIsKeptAsAnEscapeThatReadsBackAsTheSameText() {
        String lone = "a\uD800b\uDC00c";
        assertThat(JsonValues.hasUnpairedSurrogate(lone)).isTrue();
        assertThat(JsonValues.hasUnpairedSurrogate("pair 😀")).isFalse();
        assertThat(JsonValues.quotedKeepingSurrogates(lone)).isEqualTo("\"a\\uD800b\\uDC00c\"");
        assertThat(JsonValues.quotedKeepingSurrogates("😀\"\\\n\u0001" + lone))
                .isEqualTo("\"😀\\\"\\\\\\u000A\\u0001a\\uD800b\\uDC00c\"");

        assertThat(roundTrip(lone)).isEqualTo(lone);
    }

    /**
     * 09's property layer for JSON strings: random text heavy in what breaks encoders (NUL and the other controls,
     * quotes, backslashes, surrogate pairs and lone halves, RTL marks, the line and paragraph separators) is written
     * as the writer writes it and read back unchanged.
     */
    @Test
    void randomTextReadsBackUnchanged() {
        Random random = new Random(20260924);
        char[] hostile = {
            0, 1, 0x1A, 0x1F, '"', '\\', '/', '\n', '\r', '\t', 0x7F, 0x2028, 0x2029, 0x200F, 0xFEFF, 0xFFFD, 0xD800,
            0xDBFF, 0xDC00, 0xDFFF, 'a', 'Ω', 'é'
        };
        for (int n = 0; n < 2000; n++) {
            StringBuilder text = new StringBuilder();
            int length = random.nextInt(40);
            for (int i = 0; i < length; i++) {
                switch (random.nextInt(4)) {
                    case 0 -> text.append(hostile[random.nextInt(hostile.length)]);
                    case 1 -> text.appendCodePoint(0x1F600 + random.nextInt(80));
                    case 2 -> text.append((char) random.nextInt(0x10000));
                    default -> text.append((char) (' ' + random.nextInt(95)));
                }
            }
            String value = text.toString();
            assertThat(roundTrip(value))
                    .as("round trip of %s", value.codePoints().boxed().toList())
                    .isEqualTo(value);
        }
    }

    /** Writes a string as the writer's {@code text} does, as UTF-8, and parses it back. */
    private static String roundTrip(String value) {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (JsonGenerator g = JsonFormat.FACTORY.createGenerator(ObjectWriteContext.empty(), bytes)) {
            if (JsonValues.hasUnpairedSurrogate(value)) {
                g.writeRawValue(JsonValues.quotedKeepingSurrogates(value));
            } else {
                g.writeString(value);
            }
        }
        // Valid UTF-8 whatever the text held: a strict decoder accepts it
        try {
            StandardCharsets.UTF_8.newDecoder().decode(ByteBuffer.wrap(bytes.toByteArray()));
        } catch (CharacterCodingException e) {
            throw new AssertionError("not UTF-8: " + value.codePoints().boxed().toList(), e);
        }
        try (JsonParser p = JsonFormat.FACTORY.createParser(JsonFormat.reading(), bytes.toByteArray())) {
            p.nextToken();
            return p.getString();
        }
    }
}
