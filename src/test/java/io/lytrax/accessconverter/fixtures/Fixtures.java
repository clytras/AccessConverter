package io.lytrax.accessconverter.fixtures;

import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.DatabaseBuilder;
import com.healthmarketscience.jackcess.DateTimeType;
import com.healthmarketscience.jackcess.crypt.CryptCodecProvider;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Path;

/** Shared helpers for the fixture tiers of 09: A (generated), B (Jackcess corpus), C (local samples). */
public final class Fixtures {
    private Fixtures() {}

    /** {@code target/fixtures} when run by Maven ({@code -Dfixtures.dir}), else relative to the working directory. */
    public static Path root() {
        String dir = System.getProperty("fixtures.dir");
        return (dir != null ? Path.of(dir) : Path.of("target", "fixtures")).toAbsolutePath();
    }

    /** Opens an Access file read-only with {@link java.time.LocalDateTime} values (no time zone involved). */
    public static Database openReadOnly(Path file) throws IOException {
        return openReadOnly(file, null);
    }

    /** As {@link #openReadOnly(Path)}, for encoded and encrypted files too (jackcess-encrypt). */
    public static Database openReadOnly(Path file, String password) throws IOException {
        return openReadOnly(file, password, null);
    }

    /** As {@link #openReadOnly(Path, String)}, decoding an Access 97 file's text with {@code charset}. */
    public static Database openReadOnly(Path file, String password, Charset charset) throws IOException {
        Database db = new DatabaseBuilder(file)
                .setCharset(charset)
                .setReadOnly(true)
                .setCodecProvider(new CryptCodecProvider(password))
                // Some fixtures are written by a non-English Access, whose catalog index Jackcess can't use; tests
                // compare against the whole catalog, as the converter reads it
                .setIgnoreBrokenSystemCatalogIndex(true)
                .open();
        db.setDateTimeType(DateTimeType.LOCAL_DATE_TIME);
        return db;
    }

    static Database create(Path file) throws IOException {
        Database db = new DatabaseBuilder(file)
                .setFileFormat(Database.FileFormat.V2010)
                .create();
        db.setDateTimeType(DateTimeType.LOCAL_DATE_TIME);
        // Jackcess would otherwise evaluate DefaultValue on insert: a NULL Created would become Now(), a different
        // value on every build
        db.setEvaluateExpressions(false);
        return db;
    }
}
