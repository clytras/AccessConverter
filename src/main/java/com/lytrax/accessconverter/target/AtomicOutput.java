package com.lytrax.accessconverter.target;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Builds an output as {@code <output>.partial} next to it and renames it into place only when everything
 * succeeded, so a failed conversion never leaves a half-written output behind (03, finalize; F-42).
 */
public final class AtomicOutput {

    /** Builds the output at the path it is given. */
    @FunctionalInterface
    public interface Build<T> {
        T into(Path partial) throws IOException;
    }

    private AtomicOutput() {}

    public static <T> T write(Path output, Build<T> build) throws IOException {
        Path partial = output.resolveSibling(output.getFileName() + ".partial");
        Files.deleteIfExists(partial);
        try {
            T result = build.into(partial);
            Files.move(partial, output, ATOMIC_MOVE, REPLACE_EXISTING);
            return result;
        } catch (IOException | RuntimeException e) {
            try {
                Files.deleteIfExists(partial);
            } catch (IOException suppressed) {
                e.addSuppressed(suppressed);
            }
            throw e;
        }
    }
}
