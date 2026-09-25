package io.lytrax.accessconverter.source;

import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.impl.DatabaseImpl;
import java.io.IOException;

/**
 * The only use of Jackcess internals (04, Access 97 text). For Jet 3 files Jackcess decodes text with
 * {@code Charset.defaultCharset()}, which is UTF-8 on Java 18+, so non-ASCII Access 97 text comes out garbled. The
 * database's real code page is in its header, but only {@link DatabaseImpl#getDefaultCodePage()} exposes it. It's
 * kept to this class and has its own test (Jet3CodePageTest).
 */
final class Jet3CodePage {
    private Jet3CodePage() {}

    static int read(Database db) throws IOException {
        if (!(db instanceof DatabaseImpl impl)) {
            throw new IllegalStateException("unexpected Jackcess Database implementation "
                    + db.getClass().getName());
        }
        return Short.toUnsignedInt(impl.getDefaultCodePage());
    }
}
