package com.lytrax.accessconverter.target;

import java.util.Locale;

/** Where the bytes of Binary, OLE and attachment values go (08, {@code --binary}). */
public enum BinaryMode {
    /** In the output itself: a BLOB, or base64 in JSON. */
    INLINE,
    /**
     * In files under {@code <output>-files/}; the output holds each file's relative path (JSON: {@code {file, size}}).
     */
    FILES,
    /** Dropped: the output holds each value's size in bytes instead (JSON: {@code {size}}), NULL staying NULL. */
    OMIT;

    public String label() {
        return name().toLowerCase(Locale.ROOT);
    }
}
