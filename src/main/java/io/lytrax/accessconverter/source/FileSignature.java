package io.lytrax.accessconverter.source;

import io.lytrax.accessconverter.source.SourceException.Kind;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;

/**
 * Checks a file's header before Jackcess sees it, so inputs that aren't readable databases fail with a clear
 * reason. Page 0 starts with {@code 00 01 00 00}, then an identifier at 0x04 and a version byte at 0x14:
 *
 * <ul>
 *   <li>{@code Standard Jet DB}: Access 97 to 2003 (.mdb, .mde)
 *   <li>{@code Standard ACE DB}: Access 2007 and later (.accdb, .accde, .accdr)
 *   <li>{@code MSISAM Database}: Microsoft Money (.mny), which Jackcess also reads
 *   <li>{@code Jet System DB}: a workgroup information file (.mdw), which holds no data
 * </ul>
 */
final class FileSignature {
    private static final int HEADER = 0x20;
    private static final int OFFSET_IDENTIFIER = 0x04;
    private static final int OFFSET_VERSION = 0x14;

    private FileSignature() {}

    static void check(Path file) throws IOException {
        String name = file.getFileName().toString().toLowerCase(Locale.ROOT);
        if (name.endsWith(".adp")) {
            throw new SourceException(
                    Kind.ACCESS_PROJECT,
                    file,
                    "an Access project (.adp) is a front end whose tables live in SQL Server; export them from the"
                            + " server instead",
                    null);
        }
        byte[] header = new byte[HEADER];
        int read;
        try (InputStream in = Files.newInputStream(file)) {
            read = in.readNBytes(header, 0, HEADER);
        }
        if (read == 0) {
            throw new SourceException(Kind.NOT_AN_ACCESS_DATABASE, file, "the file is empty", null);
        }
        String identifier = read < OFFSET_VERSION
                ? ""
                : new String(header, OFFSET_IDENTIFIER, OFFSET_VERSION - OFFSET_IDENTIFIER, StandardCharsets.US_ASCII);
        boolean magic = read == HEADER && header[0] == 0 && header[1] == 1 && header[2] == 0 && header[3] == 0;
        if (magic && identifier.startsWith("Jet System DB")) {
            throw new SourceException(
                    Kind.WORKGROUP_FILE,
                    file,
                    "a workgroup information file (.mdw) holds Access users and permissions, not data; convert the"
                            + " database that uses it",
                    null);
        }
        if (!magic
                || !(identifier.startsWith("Standard Jet DB")
                        || identifier.startsWith("Standard ACE DB")
                        || identifier.startsWith("MSISAM Database"))) {
            throw new SourceException(
                    Kind.NOT_AN_ACCESS_DATABASE,
                    file,
                    "not an Access database (no Jet or ACE header); Access 97 through Microsoft 365 files are supported",
                    null);
        }
    }

    /** The version byte, for the error when Jackcess rejects it. */
    static int version(Path file) throws IOException {
        try (InputStream in = Files.newInputStream(file)) {
            byte[] header = in.readNBytes(HEADER);
            return header.length > OFFSET_VERSION ? Byte.toUnsignedInt(header[OFFSET_VERSION]) : -1;
        }
    }
}
