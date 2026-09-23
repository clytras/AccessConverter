package com.lytrax.accessconverter.fixtures;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.Objects;

/**
 * Tier D: small Access-authored databases committed with the tests, made in the maintainer's Windows 98 guest with
 * Access 97 ({@code win98 mkdb}) and dumped there as ground truth ({@code win98 dumpdb}). They are Greek Access 97
 * files, whose catalog index Jackcess 5.0.1 can't use and whose text is in code page 1253.
 */
public enum Access97Fixture {
    /** Three tables: every column type, Greek and ASCII text, numeric extremes, NULL against "", one relationship. */
    GR97("gr97");

    private final String name;

    Access97Fixture(String name) {
        this.name = name;
    }

    public Path file() {
        return resource(name + ".mdb");
    }

    /** What Access itself printed for this database. */
    public GuestDump dump() {
        return GuestDump.read(resource(name + ".dump.txt"));
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
