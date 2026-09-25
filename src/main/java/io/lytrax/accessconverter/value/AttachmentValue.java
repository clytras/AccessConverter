package io.lytrax.accessconverter.value;

import java.time.LocalDateTime;
import java.util.Objects;

/**
 * One file of an attachment cell (08), as Jackcess decodes it from Access's hidden attachment table.
 *
 * @param id Access's complex value id, unique across the column
 * @param data the file's bytes, decompressed; null when the row holds none
 */
public record AttachmentValue(
        int id, String fileName, String fileType, byte[] data, String url, LocalDateTime timestamp, Integer flags) {

    /** The decoded size, or null without data. */
    public Long size() {
        return data == null ? null : (long) data.length;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof AttachmentValue a
                && id == a.id
                && Objects.equals(fileName, a.fileName)
                && Objects.equals(fileType, a.fileType)
                && java.util.Arrays.equals(data, a.data)
                && Objects.equals(url, a.url)
                && Objects.equals(timestamp, a.timestamp)
                && Objects.equals(flags, a.flags);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, fileName, fileType, java.util.Arrays.hashCode(data), url, timestamp, flags);
    }

    @Override
    public String toString() {
        return "AttachmentValue[" + id + ", " + fileName + ", " + (data == null ? "no data" : data.length + " bytes")
                + "]";
    }
}
