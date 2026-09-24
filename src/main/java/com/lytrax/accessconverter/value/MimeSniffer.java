package com.lytrax.accessconverter.value;

import java.nio.charset.StandardCharsets;

/**
 * A content type from the bytes themselves, by magic number (08): PNG, JPEG, GIF, BMP, PDF, ZIP and the Office formats
 * built on ZIP or on OLE compound storage. Never from a file name, and null for anything not recognized: a guess made
 * silently is worse than a NULL the reader can see.
 */
public final class MimeSniffer {

    public static final String PNG = "image/png";
    public static final String JPEG = "image/jpeg";
    public static final String GIF = "image/gif";
    public static final String BMP = "image/bmp";
    public static final String PDF = "application/pdf";
    public static final String ZIP = "application/zip";
    public static final String DOCX = "application/vnd.openxmlformats-officedocument.wordprocessingml.document";
    public static final String XLSX = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet";
    public static final String PPTX = "application/vnd.openxmlformats-officedocument.presentationml.presentation";
    /** OLE2 compound storage: the pre-2007 Office formats (.doc, .xls, .ppt) and other OLE documents. */
    public static final String OLE_STORAGE = "application/x-ole-storage";

    private static final byte[] PNG_MAGIC = {(byte) 0x89, 'P', 'N', 'G', 0x0D, 0x0A, 0x1A, 0x0A};
    private static final byte[] OLE_MAGIC = {
        (byte) 0xD0, (byte) 0xCF, 0x11, (byte) 0xE0, (byte) 0xA1, (byte) 0xB1, 0x1A, (byte) 0xE1
    };

    /** How many ZIP entries are looked at for an Office part name. */
    private static final int ZIP_ENTRIES = 64;

    private MimeSniffer() {}

    /** The content type, or null when the bytes aren't one of the recognized formats. */
    public static String sniff(byte[] b) {
        if (b == null || b.length < 4) {
            return null;
        }
        if (startsWith(b, PNG_MAGIC)) {
            return PNG;
        }
        if ((b[0] & 0xFF) == 0xFF && (b[1] & 0xFF) == 0xD8 && (b[2] & 0xFF) == 0xFF) {
            return JPEG;
        }
        if (startsWith(b, ascii("GIF87a")) || startsWith(b, ascii("GIF89a"))) {
            return GIF;
        }
        if (startsWith(b, ascii("%PDF-"))) {
            return PDF;
        }
        if (startsWith(b, OLE_MAGIC)) {
            return OLE_STORAGE;
        }
        if (b[0] == 'P' && b[1] == 'K' && (b[2] == 3 || b[2] == 5 || b[2] == 7) && b[3] == b[2] + 1) {
            return zip(b);
        }
        if (bmp(b)) {
            return BMP;
        }
        return null;
    }

    /** A file extension for a content type, with its dot, or null. */
    public static String extension(String mime) {
        if (mime == null) {
            return null;
        }
        return switch (mime) {
            case PNG -> ".png";
            case JPEG -> ".jpg";
            case GIF -> ".gif";
            case BMP -> ".bmp";
            case PDF -> ".pdf";
            case ZIP -> ".zip";
            case DOCX -> ".docx";
            case XLSX -> ".xlsx";
            case PPTX -> ".pptx";
            case OLE_STORAGE -> ".ole";
            default -> null;
        };
    }

    /** "BM", then the file size the header states, which must be the length (or a little less: padding). */
    private static boolean bmp(byte[] b) {
        if (b.length < 26 || b[0] != 'B' || b[1] != 'M') {
            return false;
        }
        long size = le32(b, 2);
        long offset = le32(b, 10);
        long header = le32(b, 14);
        boolean knownHeader =
                header == 12 || header == 40 || header == 52 || header == 56 || header == 108 || header == 124;
        return size <= b.length && size >= 26 && offset < size && knownHeader;
    }

    /** A ZIP archive, or an Office Open XML document when its entries say so. */
    private static String zip(byte[] b) {
        int at = 0;
        for (int n = 0; n < ZIP_ENTRIES && at + 30 <= b.length; n++) {
            if (le32(b, at) != 0x04034B50L) {
                break;
            }
            int flags = le16(b, at + 6);
            long compressed = le32(b, at + 18);
            int nameLength = le16(b, at + 26);
            int extraLength = le16(b, at + 28);
            if (at + 30 + nameLength > b.length) {
                break;
            }
            String name = new String(b, at + 30, nameLength, StandardCharsets.UTF_8);
            if (name.startsWith("word/")) {
                return DOCX;
            }
            if (name.startsWith("xl/")) {
                return XLSX;
            }
            if (name.startsWith("ppt/")) {
                return PPTX;
            }
            if ((flags & 0x08) != 0) {
                break; // sizes follow the data: the next header can't be found without inflating
            }
            at += 30 + nameLength + extraLength + (int) compressed;
        }
        return ZIP;
    }

    private static boolean startsWith(byte[] b, byte[] prefix) {
        if (b.length < prefix.length) {
            return false;
        }
        for (int i = 0; i < prefix.length; i++) {
            if (b[i] != prefix[i]) {
                return false;
            }
        }
        return true;
    }

    private static byte[] ascii(String text) {
        return text.getBytes(StandardCharsets.US_ASCII);
    }

    private static int le16(byte[] b, int at) {
        return (b[at] & 0xFF) | (b[at + 1] & 0xFF) << 8;
    }

    private static long le32(byte[] b, int at) {
        return (b[at] & 0xFFL) | (b[at + 1] & 0xFFL) << 8 | (b[at + 2] & 0xFFL) << 16 | (b[at + 3] & 0xFFL) << 24;
    }
}
