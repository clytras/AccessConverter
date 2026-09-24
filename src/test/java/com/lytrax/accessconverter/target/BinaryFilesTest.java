package com.lytrax.accessconverter.target;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** 08, File-name safety: names from data never leave {@code <output>-files/} and never overwrite each other. */
class BinaryFilesTest {

    @TempDir
    Path dir;

    @Test
    void aHostileNameStaysInsideTheFilesDirectory() throws IOException {
        Path output = dir.resolve("out").resolve("db.sqlite3");
        Files.createDirectories(output.getParent());
        BinaryFiles files = new BinaryFiles(output);
        List<String> hostile = List.of(
                "..\\..\\x.txt",
                "../../x.txt",
                "/etc/passwd",
                "C:\\Windows\\win.ini",
                "..",
                ".",
                "a/../../b.txt",
                "CON",
                "nul.txt",
                "x:y*z?.txt");
        for (String name : hostile) {
            String path = files.write("..\\T", "../C", "1", name, bytes(name));
            assertThat(path)
                    .startsWith("db.sqlite3-files/")
                    .doesNotContain("..")
                    .doesNotContain("\\");
        }
        files.commit();

        Path root = output.getParent().resolve("db.sqlite3-files").toRealPath();
        try (Stream<Path> all = Files.walk(dir)) {
            List<Path> written = all.filter(Files::isRegularFile).toList();
            assertThat(written).hasSize(hostile.size());
            for (Path file : written) {
                assertThat(file.toRealPath()).startsWith(root);
            }
        }
        // Nothing escaped next to the output or above it
        try (Stream<Path> siblings = Files.list(output.getParent())) {
            assertThat(siblings.map(p -> p.getFileName().toString())).containsExactly("db.sqlite3-files");
        }
        try (Stream<Path> top = Files.list(dir)) {
            assertThat(top.map(p -> p.getFileName().toString())).containsExactly("out");
        }
    }

    @Test
    void theHostileNamesAreMadeSafe() throws IOException {
        BinaryFiles files = new BinaryFiles(dir.resolve("db.json"));
        assertThat(files.write("T", "C", "1", "..\\..\\x.txt", bytes("a"))).isEqualTo("db.json-files/T/C/1-x.txt");
        assertThat(files.write("T", "C", "2", "CON", bytes("b"))).isEqualTo("db.json-files/T/C/2-_CON");
        assertThat(files.write("T", "C", "3", "a<b>.txt", bytes("c"))).isEqualTo("db.json-files/T/C/3-a_b_.txt");
        assertThat(files.write("CON", "AUX", "4", "x.txt", bytes("d"))).isEqualTo("db.json-files/_CON/_AUX/4-x.txt");
    }

    @Test
    void aNameThatCollidesAfterSanitizingGetsASuffixNeverAnOverwrite() throws IOException {
        BinaryFiles files = new BinaryFiles(dir.resolve("db.json"));
        String first = files.write("T", "C", "1", "a:b.txt", bytes("first"));
        String second = files.write("T", "C", "1", "a*b.txt", bytes("second"));
        String third = files.write("T", "C", "1", "A_B.TXT", bytes("third"));
        files.commit();

        assertThat(first).isEqualTo("db.json-files/T/C/1-a_b.txt");
        assertThat(second).isEqualTo("db.json-files/T/C/1-a_b-2.txt");
        assertThat(third).isEqualTo("db.json-files/T/C/1-A_B-3.TXT");
        assertThat(dir.resolve(first)).hasContent("first");
        assertThat(dir.resolve(second)).hasContent("second");
        assertThat(dir.resolve(third)).hasContent("third");
    }

    @Test
    void aNamelessValueIsNamedAfterItsRowAndItsContent() throws IOException {
        BinaryFiles files = new BinaryFiles(dir.resolve("db.json"));
        byte[] png = {(byte) 0x89, 'P', 'N', 'G', 0x0D, 0x0A, 0x1A, 0x0A};
        assertThat(files.write("T", "C", "7", null, png)).isEqualTo("db.json-files/T/C/7.png");
        assertThat(files.write("T", "C", "8", null, bytes("text"))).isEqualTo("db.json-files/T/C/8.bin");
        assertThat(files.write("T", "C", "9", null, new byte[0])).isEqualTo("db.json-files/T/C/9.bin");
    }

    @Test
    void theDirectoryAppearsOnlyWhenCommittedAndReplacesAnOldOne() throws IOException {
        Path old = dir.resolve("db.json-files");
        Files.createDirectories(old.resolve("T"));
        Files.writeString(old.resolve("T").resolve("stale.bin"), "stale");

        BinaryFiles files = new BinaryFiles(dir.resolve("db.json"));
        files.write("T", "C", "1", "x.txt", bytes("new"));
        assertThat(old.resolve("T/stale.bin")).exists();
        files.commit();

        assertThat(old.resolve("T/stale.bin")).doesNotExist();
        assertThat(old.resolve("T/C/1-x.txt")).hasContent("new");
        assertThat(dir.resolve("db.json-files.partial")).doesNotExist();
    }

    @Test
    void aDiscardedConversionLeavesNothing() throws IOException {
        BinaryFiles files = new BinaryFiles(dir.resolve("db.json"));
        files.write("T", "C", "1", "x.txt", bytes("x"));
        files.discard();
        try (Stream<Path> left = Files.list(dir)) {
            assertThat(left).isEmpty();
        }
    }

    @Test
    void aDirectoryHoldingALinkIsNeverDeleted() throws IOException {
        Path old = Files.createDirectories(dir.resolve("db.json-files"));
        Path target = Files.createDirectories(dir.resolve("elsewhere"));
        try {
            Files.createSymbolicLink(old.resolve("link"), target);
        } catch (IOException | UnsupportedOperationException e) {
            return; // symbolic links need a privilege on Windows
        }
        BinaryFiles files = new BinaryFiles(dir.resolve("db.json"));
        files.write("T", "C", "1", "x.txt", bytes("x"));
        assertThatThrownBy(files::commit).isInstanceOf(IOException.class).hasMessageContaining("won't be replaced");
        assertThat(target).exists();
    }

    private static byte[] bytes(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }
}
