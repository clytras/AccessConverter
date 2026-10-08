package io.lytrax.accessconverter.source;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
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

    /**
     * Bug 010: {@link Charset#decode} takes its decoder from a per-thread cache that matches charsets by name. Once the
     * JDK charset has decoded on a thread, a charset equal to it would be handed the JDK's decoder there.
     */
    @Test
    void theJdkCharsetDecodingFirstOnTheSameThreadChangesNothing() {
        Charset cp1252 = WindowsSingleByte.of(JDK_1252);
        ByteBuffer c1 = ByteBuffer.wrap(new byte[] {(byte) 0x81});
        assertThat(JDK_1252.decode(c1.duplicate()).toString()).isEqualTo(String.valueOf(REPLACEMENT));
        assertThat(cp1252.decode(c1.duplicate()).toString()).isEqualTo("");

        Charset jdk1253 = Charset.forName("windows-1253");
        ByteBuffer privateUse = ByteBuffer.wrap(new byte[] {(byte) 0xAA, (byte) 0xD2, (byte) 0xFF});
        assertThat(jdk1253.decode(privateUse.duplicate()).toString()).isEqualTo("���");
        assertThat(WindowsSingleByte.of(jdk1253).decode(privateUse.duplicate()).toString())
                .isEqualTo("");
    }

    @Test
    void itHasANameOfItsOwnAndEncodesTheRestAsTheJdkDoes() {
        Charset cp1252 = WindowsSingleByte.of(JDK_1252);
        assertThat(cp1252).isNotEqualTo(JDK_1252).hasToString("x-windows-1252-access");
        assertThat(WindowsSingleByte.of(Charset.forName("x-windows-874"))).hasToString("x-windows-874-access");
        assertThat(WindowsSingleByte.base(cp1252)).isSameAs(JDK_1252);
        assertThat(WindowsSingleByte.base(StandardCharsets.UTF_16LE)).isSameAs(StandardCharsets.UTF_16LE);
        assertThat(cp1252.contains(StandardCharsets.US_ASCII)).isTrue();
        assertThat(cp1252.contains(JDK_1252)).isTrue();
        assertThat("Ωx😀".getBytes(cp1252)).isEqualTo("?x?".getBytes(StandardCharsets.US_ASCII));
    }

    @Test
    void multiByteCharsetsAreLeftAlone() {
        Charset sjis = Charset.forName("windows-31j");
        assertThat(WindowsSingleByte.of(sjis)).isSameAs(sjis);
        assertThat(WindowsSingleByte.of(StandardCharsets.UTF_8)).isSameAs(StandardCharsets.UTF_8);
    }
}
