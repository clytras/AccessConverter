package io.lytrax.accessconverter.source;

import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.impl.DatabaseImpl;
import java.io.IOException;

/**
 * The only use of Jackcess internals (04, Access 97 text). Jet 3 text is in the database's code page, which is in its
 * header, but only {@link DatabaseImpl#getDefaultCodePage()} exposes it. Jackcess 5.0.1 decoded it with {@code
 * Charset.defaultCharset()} (UTF-8 on Java 18+); 5.0.3 uses the code page's JDK charset, which still turns the bytes
 * the JDK leaves undefined into U+FFFD, so the source decodes with {@link WindowsSingleByte} instead. It's kept to this
 * class and has its own test (Jet3CodePageTest).
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
