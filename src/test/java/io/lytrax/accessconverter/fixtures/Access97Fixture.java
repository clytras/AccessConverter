package io.lytrax.accessconverter.fixtures;

import io.lytrax.accessconverter.source.OpenOptions;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.Objects;

/**
 * Tier D: small Access-authored databases committed with the tests, made in the maintainer's Windows 98 guest with
 * Access 97 ({@code win98 mkdb}, or Access's own designer) and dumped there as ground truth ({@code win98 dumpdb}).
 * The {@code gr97} files are Greek Access 97 files, whose catalog index Jackcess 5.0.1 can't use and whose text is in
 * code page 1253.
 */
public enum Access97Fixture {
    /** Three tables: every column type, Greek and ASCII text, numeric extremes, NULL against "", one relationship. */
    GR97("gr97", "gr97", null),
    /**
     * The same database, encoded (DAO's {@code dbEncrypt}, so it can only be read through a codec provider) and
     * given a database password, keeping the Greek collation whose catalog index Jackcess can't use. It is what a
     * protected database from a non-English Access 97 looks like: every part of opening it at once.
     */
    GR97_ENC("gr97enc", "gr97", "gr97pass"),
    /**
     * A Random autonumber made in Access 97's own table designer (New Values: Random), which Access stores as the
     * default {@code GenUniqueID()} on an ordinary Long autonumber, beside an Increment one. Jet generated its values
     * (two of them negative); a fourth row sits at INT's ceiling, 2147483647. English collation, so unlike the Greek
     * files its catalog index is usable. The primary keys, the Increment table and the ceiling row were added through
     * DAO 3.51, which stores them as Access does.
     */
    RANDOM_AUTO_NUMBER("randomAutoNumber", "randomAutoNumber", null);

    private final String name;
    private final String dumpName;
    private final String password;

    Access97Fixture(String name, String dumpName, String password) {
        this.name = name;
        this.dumpName = dumpName;
        this.password = password;
    }

    public Path file() {
        return resource(name + ".mdb");
    }

    public String password() {
        return password;
    }

    public OpenOptions openOptions() {
        return new OpenOptions(password, null);
    }

    /** What Access itself printed for this database; the encoded copy holds the same rows as the plain one. */
    public GuestDump dump() {
        return GuestDump.read(resource(dumpName + ".dump.txt"));
    }

    private static Path resource(String fileName) {
        String path = "/fixtures/access97/" + fileName;
        try {
            return Path.of(Objects.requireNonNull(Access97Fixture.class.getResource(path), path)
                    .toURI());
        } catch (URISyntaxException e) {
            throw new AssertionError(e);
        }
    }
}
