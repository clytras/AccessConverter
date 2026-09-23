package com.lytrax.accessconverter.source;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Objects;

/**
 * A database that can't be read, with the reason as a {@link Kind}. The message is written for the user: the CLI
 * prints it as is and exits with 2, never with a stack trace.
 */
public final class SourceException extends IOException {
    private static final long serialVersionUID = 1L;

    public enum Kind {
        /** No Jet/ACE/MSISAM header: not an Access database at all (or empty). */
        NOT_AN_ACCESS_DATABASE,
        /** A workgroup information file (.mdw): users and permissions, no data. */
        WORKGROUP_FILE,
        /** An Access project (.adp): a front end whose data lives in SQL Server. */
        ACCESS_PROJECT,
        /** A Jet/ACE header with a version Jackcess can't read, such as Access 1.x/2.0 or a future format. */
        UNSUPPORTED_VERSION,
        /** Encrypted, and no password was given. */
        PASSWORD_REQUIRED,
        /** Encrypted, and the password is wrong. */
        WRONG_PASSWORD,
        /** Encrypted with a method that can't be decrypted. */
        UNSUPPORTED_ENCRYPTION,
        /** The file is damaged or truncated. */
        CORRUPT,
        /** Opened, but reading a table failed. */
        READ_FAILED
    }

    private final Kind kind;

    SourceException(Kind kind, Path file, String message, Throwable cause) {
        super(file.getFileName() + ": " + message, cause);
        this.kind = Objects.requireNonNull(kind, "kind");
    }

    public Kind kind() {
        return kind;
    }

    /** A failure while reading, with the underlying error named so it can be diagnosed without a stack trace. */
    public static SourceException readFailed(Path file, String what, Throwable cause) {
        return new SourceException(
                Kind.READ_FAILED,
                file,
                "reading " + what + " failed (" + describe(cause)
                        + "); the file may be damaged: try Compact and Repair in Access",
                cause);
    }

    static String describe(Throwable e) {
        String message = e.getMessage();
        return e.getClass().getSimpleName() + (message == null ? "" : ": " + message);
    }
}
