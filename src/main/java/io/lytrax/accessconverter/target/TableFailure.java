package io.lytrax.accessconverter.target;

import io.lytrax.accessconverter.report.IssueCode;
import io.lytrax.accessconverter.report.Issues;
import io.lytrax.accessconverter.source.SourceException;
import io.lytrax.accessconverter.target.ConvertOptions.OnTableError;
import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * A table that couldn't be read or written (03, Error handling): reported as {@code TABLE_READ_FAILED} or
 * {@code TABLE_WRITE_FAILED}, then either the conversion stops ({@code --on-table-error fail}) or the other tables
 * go on ({@code continue}).
 */
public final class TableFailure {

    private TableFailure() {}

    /**
     * Reports a failed table and, under {@code --on-table-error fail}, throws; under {@code continue} it returns and
     * the writer goes on with the next table.
     *
     * @param rowsWritten rows of the table already written when it failed
     */
    public static void handle(Issues issues, ConvertOptions options, String table, long rowsWritten, Exception e)
            throws IOException {
        if (e instanceof AlreadyReported) {
            return;
        }
        handle(issues, options, table, rowsWritten, e, e instanceof IOException || e instanceof UncheckedIOException);
    }

    /**
     * A table the profiling pass couldn't read (04): reported as {@code TABLE_READ_FAILED} with the message a writer
     * gives, whatever the exception, and handled as {@link #handle} does.
     */
    public static void readFailed(Issues issues, ConvertOptions options, String table, Exception e) throws IOException {
        handle(issues, options, table, 0, e instanceof UncheckedIOException u ? u.getCause() : e, true);
    }

    private static void handle(
            Issues issues, ConvertOptions options, String table, long rowsWritten, Exception e, boolean reading)
            throws IOException {
        issues.add(
                reading ? IssueCode.TABLE_READ_FAILED : IssueCode.TABLE_WRITE_FAILED,
                table,
                null,
                (reading ? "reading" : "writing") + " the table failed after " + rowsWritten + " rows: " + message(e));
        if (options.onTableError() == OnTableError.FAIL) {
            throw e instanceof SourceException source
                    ? source
                    : new IOException("table " + table + " failed: " + message(e), e);
        }
    }

    /**
     * What a writer's row source throws for a table that failed while profiling, under {@code --on-table-error
     * continue}: the writer handles it like any failed table and writes the table empty, and {@link #handle} doesn't
     * report it again.
     */
    public static final class AlreadyReported extends RuntimeException {
        private static final long serialVersionUID = 1L;

        public AlreadyReported(String table) {
            super("table " + table + " failed while profiling", null, false, false);
        }
    }

    /** The message a user should see: the source's own when a read failed, else the exception's. */
    public static String message(Throwable e) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t instanceof SourceException source) {
                return source.getMessage();
            }
        }
        return e.getClass().getSimpleName() + (e.getMessage() == null ? "" : ": " + e.getMessage());
    }
}
