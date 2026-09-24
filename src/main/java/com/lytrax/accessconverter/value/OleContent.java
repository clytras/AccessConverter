package com.lytrax.accessconverter.value;

import java.util.Locale;

/**
 * What an OLE Object value holds once decoded (08, {@code --ole-extract}). Decoding is additive: the raw bytes stay the
 * source of truth, and a value that can't be decoded keeps them with {@link #problem} set and every field null.
 *
 * @param kind null when the value couldn't be decoded
 * @param name package: the file name; embedded and compound: the OLE class name (such as {@code PBrush}); link: the
 *     link path; raw: null
 * @param mime the content's type, sniffed from its bytes by magic number only ({@link MimeSniffer}); null when the
 *     bytes aren't recognized or there are none
 * @param content package: the embedded file; embedded and compound: the object's own bytes; link and raw: null (a raw
 *     value's bytes are the OLE column itself)
 * @param problem why the value couldn't be decoded, or null
 */
public record OleContent(Kind kind, String name, String mime, byte[] content, String problem) {

    public enum Kind {
        /** An OLE package holding a file (Jackcess's simple package). */
        PACKAGE,
        /** An embedded OLE object of some application (Word, Excel, Paintbrush). */
        EMBEDDED,
        /** A link to a file outside the database. */
        LINK,
        /** An OLE compound storage document. */
        COMPOUND,
        /** Bytes that are not OLE-wrapped at all, as applications that stored images directly wrote them. */
        RAW;

        /** The spelling in every output: {@code package}, {@code embedded}, … */
        public String label() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    public static OleContent undecodable(String problem) {
        return new OleContent(null, null, null, null, problem);
    }

    public boolean decoded() {
        return kind != null;
    }
}
