package io.lytrax.accessconverter.target;

import io.lytrax.accessconverter.source.SourceException;
import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;

/**
 * An output that can't be written, named as the user gave it, with the reason in words: {@code <output>: <reason>}.
 * The CLI prints the message as is and exits with 2, as it does for a {@link SourceException}.
 */
public final class OutputException extends IOException {
    private static final long serialVersionUID = 1L;

    /** SQLite's primary result code for a full disk ({@code SQLITE_FULL}). */
    private static final int SQLITE_FULL = 13;

    public OutputException(Path output, String reason, Throwable cause) {
        super(output + ": " + reason, cause);
    }

    /**
     * What a failure writing {@code output} is to the user: a full disk, or a file-system error on the output or on
     * {@code written} (the path actually being written, such as its {@code .partial}), in words and under the
     * output's own name. {@code file} is whether the output is a file, not a directory of files. Anything else, a
     * failure reading the source included, is returned unchanged.
     */
    public static IOException of(Path output, Path written, boolean file, IOException e) {
        if (e instanceof OutputException || causedBy(e, SourceException.class)) {
            return e;
        }
        if (diskFull(e)) {
            return new OutputException(output, "the disk is full", e);
        }
        if (e instanceof FileSystemException fs && about(fs, output, written)) {
            String reason = folderProblem(output, file);
            if (reason == null) {
                reason = e instanceof AccessDeniedException
                        ? "no permission to write it"
                        : fs.getReason() != null ? fs.getReason() : "it could not be written";
            }
            return new OutputException(output, reason, e);
        }
        return e;
    }

    /**
     * Why {@code output} can't be created, as far as the file system tells: its folder is missing, not a folder or
     * not writable, or, for an output that is a {@code file}, a folder has its name. Null when none of these is the
     * case.
     */
    public static String folderProblem(Path output, boolean file) {
        Path absolute = output.toAbsolutePath();
        Path folder = absolute.getParent();
        // The folder as the user wrote it, or the absolute one when they gave only a file name
        Path shown = output.getParent() != null ? output.getParent() : folder;
        if (folder != null && !Files.exists(folder)) {
            return "the folder " + shown + " doesn't exist";
        }
        if (folder != null && !Files.isDirectory(folder)) {
            return shown + " is not a folder";
        }
        if (file && Files.isDirectory(absolute)) {
            return "it is a folder";
        }
        if (folder != null && !Files.isWritable(folder)) {
            return "no permission to write in the folder " + shown;
        }
        return null;
    }

    /** Whether the disk filled up, as Java or SQLite reports it, anywhere in the chain of causes. */
    public static boolean diskFull(Throwable e) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t instanceof SQLException sql && (sql.getErrorCode() & 0xff) == SQLITE_FULL) {
                return true;
            }
            String message = t.getMessage();
            if (message != null
                    && (message.contains("No space left on device")
                            || message.contains("not enough space on the disk"))) {
                return true;
            }
        }
        return false;
    }

    private static boolean about(FileSystemException e, Path output, Path written) {
        return concerns(e.getFile(), output, written) || concerns(e.getOtherFile(), output, written);
    }

    private static boolean concerns(String file, Path output, Path written) {
        if (file == null) {
            return false;
        }
        Path path = Path.of(file).toAbsolutePath().normalize();
        return path.equals(output.toAbsolutePath().normalize())
                || path.startsWith(written.toAbsolutePath().normalize());
    }

    private static boolean causedBy(Throwable e, Class<? extends Throwable> type) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (type.isInstance(t)) {
                return true;
            }
        }
        return false;
    }
}
