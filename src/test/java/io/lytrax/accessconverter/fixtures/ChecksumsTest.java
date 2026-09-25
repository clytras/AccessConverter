package io.lytrax.accessconverter.fixtures;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import net.jqwik.api.Example;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;
import net.jqwik.api.constraints.Size;

/** The tier B download is only trusted after {@link Checksums#matches}, so it must reject any altered file. */
class ChecksumsTest {

    @Example
    void knownVector() throws IOException {
        Path file = write("abc".getBytes(StandardCharsets.US_ASCII));
        assertThat(Checksums.sha256(file))
                .isEqualTo("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
    }

    @Property(tries = 200)
    void rejectsAnySingleByteChangeOrTruncation(
            @ForAll @Size(min = 1, max = 4096) byte[] content,
            @ForAll int positionSeed,
            @ForAll @IntRange(min = 1, max = 255) int flip)
            throws IOException {
        int position = Math.floorMod(positionSeed, content.length);
        Path file = write(content);
        long size = content.length;
        String sha256 = Checksums.sha256(file);
        assertThat(Checksums.matches(file, size, sha256)).isTrue();

        byte[] altered = content.clone();
        altered[position] ^= (byte) flip;
        assertThat(Checksums.matches(write(altered), size, sha256)).isFalse();
        assertThat(Checksums.matches(write(Arrays.copyOf(content, position)), size, sha256))
                .isFalse();
    }

    private static Path write(byte[] content) throws IOException {
        Path file = Files.createTempFile("checksums", ".bin");
        file.toFile().deleteOnExit();
        return Files.write(file, content);
    }
}
