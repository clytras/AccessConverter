package com.lytrax.accessconverter;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Golden files under {@code src/test/resources/golden/}: expected output compared byte for byte. Run with
 * {@code -Dgolden.update=true} to rewrite them, then review the diff.
 */
public final class Golden {
    private Golden() {}

    public static void assertMatches(String relativePath, String actual) {
        Path file = Path.of(System.getProperty("basedir", "."), "src", "test", "resources", "golden")
                .resolve(relativePath);
        try {
            if (Boolean.getBoolean("golden.update")) {
                Files.createDirectories(file.getParent());
                Files.writeString(file, actual, StandardCharsets.UTF_8);
                return;
            }
            assertThat(file)
                    .as("golden file missing; run with -Dgolden.update=true to create it")
                    .exists();
            assertThat(actual).isEqualTo(Files.readString(file, StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
