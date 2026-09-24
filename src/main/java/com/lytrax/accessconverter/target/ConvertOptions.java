package com.lytrax.accessconverter.target;

import java.util.Objects;

/**
 * The convert options every target shares (02, CLI).
 *
 * @param profile whether the profiling pass runs; without it a target makes the conservative choice everywhere
 * @param includeHidden keep Access's system-maintained columns (replication's {@code s_GUID} and friends)
 * @param onTableError what a table that can't be read or written does to the conversion
 * @param batchRows rows per {@code executeBatch} / per INSERT statement
 * @param binary where the bytes of Binary, OLE and attachment values go (08)
 * @param oleExtract also decode OLE values: their kind, name, content type and content (08)
 * @param versionHistory write the version history of append-only memos, which is skipped otherwise (08)
 */
public record ConvertOptions(
        boolean profile,
        boolean includeHidden,
        OnTableError onTableError,
        int batchRows,
        BinaryMode binary,
        boolean oleExtract,
        boolean versionHistory) {

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
        Objects.requireNonNull(binary, "binary");
    }

    /** Binary values inline, OLE values raw, no version history: 08's defaults. */
    public ConvertOptions(boolean profile, boolean includeHidden, OnTableError onTableError, int batchRows) {
        this(profile, includeHidden, onTableError, batchRows, BinaryMode.INLINE, false, false);
    }

    public ConvertOptions withBinary(BinaryMode mode, boolean extract, boolean history) {
        return new ConvertOptions(profile, includeHidden, onTableError, batchRows, mode, extract, history);
    }
}
