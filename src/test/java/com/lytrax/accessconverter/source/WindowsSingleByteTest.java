package com.lytrax.accessconverter.source;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class WindowsSingleByteTest {
    private static final Charset JDK_1252 = Charset.forName("windows-1252");
    private static final char REPLACEMENT = (char) 0xFFFD;

    @Test
    void theBytesTheJdkLeavesUndefinedDecodeToC1Controls() {
        Charset cp1252 = WindowsSingleByte.of(JDK_1252);
        byte[] bytes = {(byte) 0x80, (byte) 0x81, (byte) 0x8D, (byte) 0x8F, (byte) 0x90, (byte) 0x9D, (byte) 0xA3};
        assertThat(new String(bytes, cp1252)).isEqualTo("€\u0081\u008D\u008F\u0090\u009D£");
    }

    /** Where Windows sends a byte to its private use area, the byte stays undecodable (and is counted). */
    @Test
    void undefinedBytesAbove0x9FStayUndecodable() {
        Charset cp1253 = WindowsSingleByte.of(Charset.forName("windows-1253"));
        assertThat(new String(new byte[] {(byte) 0xAA, (byte) 0x81, (byte) 0xC1}, cp1253))
                .isEqualTo(REPLACEMENT + "\u0081Α");
    }

    @ParameterizedTest
    @ValueSource(strings = {"windows-1250", "windows-1251", "windows-1252", "windows-1254", "windows-1258"})
    void everyByteRoundTrips(String name) {
        Charset charset = WindowsSingleByte.of(Charset.forName(name));
        for (int b = 0; b < 256; b++) {
            byte[] one = {(byte) b};
            String decoded = new String(one, charset);
            assertThat(decoded).as("%s 0x%02X", name, b).doesNotContain(String.valueOf(REPLACEMENT));
            assertThat(decoded.getBytes(charset)).as("%s 0x%02X", name, b).containsExactly(one);
        }
    }

    @Test
    void itKeepsTheJdkNameAndEncodesTheRestAsTheJdkDoes() {
        Charset cp1252 = WindowsSingleByte.of(JDK_1252);
        assertThat(cp1252).isEqualTo(JDK_1252).hasToString("windows-1252");
        assertThat(cp1252.contains(StandardCharsets.US_ASCII)).isTrue();
        assertThat("Ωx😀".getBytes(cp1252)).isEqualTo("?x?".getBytes(StandardCharsets.US_ASCII));
    }

    @Test
    void multiByteCharsetsAreLeftAlone() {
        Charset sjis = Charset.forName("windows-31j");
        assertThat(WindowsSingleByte.of(sjis)).isSameAs(sjis);
        assertThat(WindowsSingleByte.of(StandardCharsets.UTF_8)).isSameAs(StandardCharsets.UTF_8);
    }
}
