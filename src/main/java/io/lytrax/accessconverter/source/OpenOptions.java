package io.lytrax.accessconverter.source;

import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.Objects;

/**
 * @param password for encrypted databases; unused otherwise
 * @param charset Access 97 only: decode text with this instead of the header's code page, in the input and in every
 *     back-end a linked table is read from
 * @param links what happens to linked tables
 */
public record OpenOptions(String password, Charset charset, Links links) {
    public static final OpenOptions DEFAULT = new OpenOptions(null, null);

    public OpenOptions {
        Objects.requireNonNull(links, "links");
    }

    public OpenOptions(String password, Charset charset) {
        this(password, charset, Links.SKIP);
    }

    /**
     * Linked tables: skipped and reported (the default), or read from their back-end (D11).
     *
     * @param resolve read a table linked to another Access file from that file
     * @param root the only directory a back-end is looked for in, by the file name of the stored path; null for the
     *     input's own directory
     */
    public record Links(boolean resolve, Path root) {
        public static final Links SKIP = new Links(false, null);

        public static Links resolve(Path root) {
            return new Links(true, root);
        }
    }

    @Override
    public String toString() {
        return "OpenOptions[password=" + (password == null ? "none" : "***") + ", charset=" + charset + ", links="
                + links + "]";
    }
}
