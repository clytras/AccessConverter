package com.lytrax.accessconverter.target;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.function.Predicate;

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

    /**
     * As {@link #write}, for an output that is a directory of files: it is built as {@code <output>.partial/} and
     * renamed into place. An existing output is replaced only when {@code ours} says it holds nothing but files this
     * tool writes, so a mistyped {@code -o} can never empty someone's directory.
     *
     * @param ours whether a file name is one this output consists of
     */
    public static <T> T writeDirectory(Path output, Predicate<String> ours, Build<T> build) throws IOException {
        Path partial = output.resolveSibling(output.getFileName() + ".partial");
        Path old = output.resolveSibling(output.getFileName() + ".old");
        removeOwn(partial, ours);
        removeOwn(old, ours);
        if (Files.exists(output)) {
            checkOwn(output, ours);
        }
        Files.createDirectory(partial);
        T result;
        try {
            result = build.into(partial);
        } catch (IOException | RuntimeException e) {
            try {
                removeOwn(partial, ours);
            } catch (IOException suppressed) {
                e.addSuppressed(suppressed);
            }
            throw e;
        }
        if (Files.exists(output)) {
            Files.move(output, old, ATOMIC_MOVE);
            Files.move(partial, output, ATOMIC_MOVE);
            removeOwn(old, ours);
        } else {
            Files.move(partial, output, ATOMIC_MOVE);
        }
        return result;
    }

    private static void checkOwn(Path directory, Predicate<String> ours) throws IOException {
        if (!Files.isDirectory(directory)) {
            throw new IOException(directory + " exists and is not a directory; it won't be replaced");
        }
        try (var entries = Files.list(directory)) {
            for (Path entry : (Iterable<Path>) entries::iterator) {
                if (!Files.isRegularFile(entry, LinkOption.NOFOLLOW_LINKS)
                        || !ours.test(entry.getFileName().toString())) {
                    throw new IOException(directory + " holds " + entry.getFileName()
                            + ", which this conversion doesn't write; it won't be replaced");
                }
            }
        }
    }

    private static void removeOwn(Path directory, Predicate<String> ours) throws IOException {
        if (!Files.exists(directory, LinkOption.NOFOLLOW_LINKS)) {
            return;
        }
        checkOwn(directory, ours);
        try (var entries = Files.list(directory)) {
            for (Path entry : (Iterable<Path>) entries::iterator) {
                Files.delete(entry);
            }
        }
        Files.delete(directory);
    }
}
