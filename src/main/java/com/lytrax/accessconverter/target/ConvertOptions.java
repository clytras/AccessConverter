package com.lytrax.accessconverter.target;

/**
 * The convert options every target shares (02, CLI).
 *
 * @param profile whether the profiling pass runs; without it a target makes the conservative choice everywhere
 * @param includeHidden keep Access's system-maintained columns (replication's {@code s_GUID} and friends)
 * @param onTableError what a table that can't be read or written does to the conversion
 * @param batchRows rows per {@code executeBatch} / per INSERT statement
 */
public record ConvertOptions(boolean profile, boolean includeHidden, OnTableError onTableError, int batchRows) {

    public static final int DEFAULT_BATCH_ROWS = 1000;

    public static final ConvertOptions DEFAULT = new ConvertOptions(true, false, OnTableError.FAIL, DEFAULT_BATCH_ROWS);

    public enum OnTableError {
        /** Stop, delete the partial output and exit 2. */
        FAIL,
        /** Finish the other tables, keep the output and exit 2. */
        CONTINUE
    }

    public ConvertOptions {
        if (batchRows < 1) {
            throw new IllegalArgumentException("batchRows must be at least 1");
        }
    }
}
