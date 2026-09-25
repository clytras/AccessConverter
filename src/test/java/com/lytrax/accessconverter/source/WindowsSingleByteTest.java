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

    /**
     * The bytes above 0x9F the JDK leaves undefined decode as Windows decodes them, measured with MultiByteToWideChar:
     * Access 97 returned U+F8F9 for 1253's 0xAA in the Greek Northwind ("5ª Ave." stored in a Greek database).
     */
    @Test
    void undefinedBytesAbove0x9FDecodeAsWindowsDecodesThem() {
        assertThat(decode("windows-1253", 0xAA, 0x81, 0xC1, 0xD2, 0xFF)).isEqualTo("\u0081Α");
        assertThat(decode("windows-1255", 0xCA, 0xD9, 0xFF)).isEqualTo("ֺ");
        assertThat(decode("windows-1257", 0xA1, 0xA5)).isEqualTo("");
        assertThat(decode("x-windows-874", 0xDB, 0xFF)).isEqualTo("");
    }

    private static String decode(String charset, int... bytes) {
        byte[] b = new byte[bytes.length];
        for (int i = 0; i < bytes.length; i++) {
            b[i] = (byte) bytes[i];
        }
        return new String(b, WindowsSingleByte.of(Charset.forName(charset)));
    }

    /** Every byte of every single-byte code page Access 97 uses decodes, and encodes back to itself. */
    @ParameterizedTest
    @ValueSource(
            strings = {
                "x-windows-874",
                "windows-1250",
                "windows-1251",
                "windows-1252",
                "windows-1253",
                "windows-1254",
                "windows-1255",
                "windows-1256",
                "windows-1257",
                "windows-1258",
                "IBM437",
                "IBM850",
                "IBM852",
                "IBM866"
            })
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
