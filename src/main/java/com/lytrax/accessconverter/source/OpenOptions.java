package com.lytrax.accessconverter.source;

import java.nio.charset.Charset;

/**
 * @param password for encrypted databases; unused otherwise
 * @param charset Access 97 only: decode text with this instead of the header's code page
 */
public record OpenOptions(String password, Charset charset) {
    public static final OpenOptions DEFAULT = new OpenOptions(null, null);

    @Override
    public String toString() {
        return "OpenOptions[password=" + (password == null ? "none" : "***") + ", charset=" + charset + "]";
    }
}
