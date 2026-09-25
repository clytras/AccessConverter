package io.lytrax.accessconverter.value;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.junit.jupiter.api.Test;

/** 08: a content type from the bytes' magic number only, and null when they aren't recognized. */
class MimeSnifferTest {

    @Test
    void theRecognizedFormatsByTheirMagicNumbers() throws IOException {
        assertThat(MimeSniffer.sniff(bytes(0x89, 'P', 'N', 'G', 0x0D, 0x0A, 0x1A, 0x0A, 0, 0)))
                .isEqualTo("image/png");
        assertThat(MimeSniffer.sniff(bytes(0xFF, 0xD8, 0xFF, 0xE0, 0, 0x10))).isEqualTo("image/jpeg");
        assertThat(MimeSniffer.sniff(ascii("GIF89a\u0001\u0000"))).isEqualTo("image/gif");
        assertThat(MimeSniffer.sniff(ascii("GIF87a\u0001\u0000"))).isEqualTo("image/gif");
        assertThat(MimeSniffer.sniff(ascii("%PDF-1.7\n"))).isEqualTo("application/pdf");
        assertThat(MimeSniffer.sniff(bytes(0xD0, 0xCF, 0x11, 0xE0, 0xA1, 0xB1, 0x1A, 0xE1, 0)))
                .isEqualTo("application/x-ole-storage");
        assertThat(MimeSniffer.sniff(zip("data.csv"))).isEqualTo("application/zip");
        assertThat(MimeSniffer.sniff(zip("word/document.xml")))
                .isEqualTo("application/vnd.openxmlformats-officedocument.wordprocessingml.document");
        assertThat(MimeSniffer.sniff(zip("xl/workbook.xml")))
                .isEqualTo("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
        assertThat(MimeSniffer.sniff(bmp())).isEqualTo("image/bmp");
    }

    @Test
    void unrecognizedOrTooShortBytesHaveNoType() {
        assertThat(MimeSniffer.sniff(null)).isNull();
        assertThat(MimeSniffer.sniff(new byte[0])).isNull();
        assertThat(MimeSniffer.sniff(ascii("hello ole"))).isNull();
        assertThat(MimeSniffer.sniff(ascii("GIF8"))).isNull();
        // "BM" alone is common in text: without a plausible bitmap header it's nothing
        assertThat(MimeSniffer.sniff(ascii("BMW makes cars, not bitmaps at all")))
                .isNull();
    }

    @Test
    void aFileNameNeverDecides() {
        // The bytes of a text file whose name says image are still unrecognized
        assertThat(MimeSniffer.sniff(ascii("photo.png"))).isNull();
    }

    @Test
    void extensionsForFileNames() {
        assertThat(MimeSniffer.extension("image/png")).isEqualTo(".png");
        assertThat(MimeSniffer.extension("application/x-ole-storage")).isEqualTo(".ole");
        assertThat(MimeSniffer.extension(null)).isNull();
    }

    private static byte[] bmp() {
        byte[] b = new byte[58];
        b[0] = 'B';
        b[1] = 'M';
        b[2] = 58; // file size
        b[10] = 54; // pixel data offset
        b[14] = 40; // BITMAPINFOHEADER
        return b;
    }

    private static byte[] zip(String entry) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (ZipOutputStream zip = new ZipOutputStream(out)) {
            zip.putNextEntry(new ZipEntry(entry));
            zip.write(ascii("x"));
            zip.closeEntry();
        }
        return out.toByteArray();
    }

    private static byte[] ascii(String text) {
        return text.getBytes(StandardCharsets.ISO_8859_1);
    }

    private static byte[] bytes(int... values) {
        byte[] b = new byte[values.length];
        for (int i = 0; i < values.length; i++) {
            b[i] = (byte) values[i];
        }
        return b;
    }
}
