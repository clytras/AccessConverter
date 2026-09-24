package com.lytrax.accessconverter.source;

import com.healthmarketscience.jackcess.util.OleBlob;
import com.lytrax.accessconverter.value.MimeSniffer;
import com.lytrax.accessconverter.value.OleContent;
import com.lytrax.accessconverter.value.OleContent.Kind;
import java.io.IOException;
import java.io.InputStream;

/**
 * Decodes an OLE Object value with Jackcess's {@link OleBlob} (08, {@code --ole-extract}): what kind of object it is,
 * its name, and the content inside the OLE wrapper. Bytes that aren't OLE-wrapped at all are {@code raw}. A value
 * Jackcess can't parse is undecodable, never an error: its raw bytes are exported regardless.
 */
public final class OleDecoder {

    private OleDecoder() {}

    public static OleContent decode(byte[] raw) {
        if (raw.length == 0) {
            return new OleContent(Kind.RAW, null, null, null, null);
        }
        try (OleBlob blob = OleBlob.Builder.fromInternalData(raw)) {
            OleBlob.Content content = blob.getContent();
            return switch (content.getType()) {
                case SIMPLE_PACKAGE -> {
                    OleBlob.SimplePackageContent pkg = (OleBlob.SimplePackageContent) content;
                    byte[] file = bytes(pkg);
                    yield new OleContent(Kind.PACKAGE, pkg.getFileName(), MimeSniffer.sniff(file), file, null);
                }
                case OTHER -> {
                    OleBlob.OtherContent other = (OleBlob.OtherContent) content;
                    byte[] object = bytes(other);
                    yield new OleContent(Kind.EMBEDDED, className(other), MimeSniffer.sniff(object), object, null);
                }
                case COMPOUND_STORAGE -> {
                    OleBlob.CompoundContent compound = (OleBlob.CompoundContent) content;
                    byte[] storage = bytes(compound);
                    yield new OleContent(Kind.COMPOUND, className(compound), MimeSniffer.sniff(storage), storage, null);
                }
                case LINK -> new OleContent(Kind.LINK, ((OleBlob.LinkContent) content).getLinkPath(), null, null, null);
                case UNKNOWN -> new OleContent(Kind.RAW, null, MimeSniffer.sniff(raw), null, null);
            };
        } catch (IOException | RuntimeException e) {
            return OleContent.undecodable(SourceException.describe(e));
        }
    }

    private static byte[] bytes(OleBlob.EmbeddedContent content) throws IOException {
        try (InputStream in = content.getStream()) {
            return in.readAllBytes();
        }
    }

    /** The OLE class (such as {@code Word.Document.8} or {@code PBrush}), else whatever name the object gives. */
    private static String className(OleBlob.PackageContent content) {
        for (String name : new String[] {content.getClassName(), content.getTypeName(), content.getPrettyName()}) {
            if (name != null && !name.isBlank()) {
                return name;
            }
        }
        return null;
    }
}
