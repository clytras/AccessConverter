package com.lytrax.accessconverter.value;

import java.util.Arrays;
import java.util.Objects;
import java.util.function.Function;

/**
 * An OLE Object value: its exact stored bytes, whatever the content (08). Decoding, when asked for, is additive and
 * never replaces these bytes.
 */
public final class OleValue {
    private final byte[] raw;
    private OleContent decoded;

    public OleValue(byte[] raw) {
        this.raw = Objects.requireNonNull(raw, "raw");
    }

    /** The stored bytes. Not copied: callers must not modify the array. */
    public byte[] raw() {
        return raw;
    }

    /**
     * The decoded content, decoded once however many companion columns ask for it.
     *
     * @param decoder the source's OLE decoder (the value layer doesn't read Access formats itself)
     */
    public OleContent decoded(Function<byte[], OleContent> decoder) {
        if (decoded == null) {
            decoded = decoder.apply(raw);
        }
        return decoded;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof OleValue other && Arrays.equals(raw, other.raw);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(raw);
    }

    @Override
    public String toString() {
        return "OleValue[" + raw.length + " bytes]";
    }
}
