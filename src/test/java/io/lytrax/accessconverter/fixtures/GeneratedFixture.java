package io.lytrax.accessconverter.fixtures;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

import com.healthmarketscience.jackcess.Database;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Tier A: Access files generated with Jackcess at test time, ported from {@code docs/plans/audit-tooling}. Each
 * file is rebuilt once per JVM into {@code target/fixtures/generated/}, so a builder change always takes effect.
 */
public enum GeneratedFixture {
    /** Keys, relationships, awkward values, Binary/OLE, a 70-column table, Greek names. */
    SCHEMA_FIDELITY("schemaFidelity.accdb", SchemaFidelityFixture::build),
    /** Strings that stress SQL literal escaping (F-02). */
    TEXT_EDGE("textEdge.accdb", TextEdgeFixture::build),
    /** A table with exactly 100 rows, the v2 MySQL batch boundary (F-19). */
    HUNDRED_ROWS("hundred.accdb", HundredRowsFixture::build),
    /** One unique index per probe pair, holding every value Access accepts (F-36). */
    UNIQUE_PROBE("uniqueProbe.accdb", UniqueProbeFixture::build);

    private final String fileName;
    private final Builder builder;
    private Path built;

    GeneratedFixture(String fileName, Builder builder) {
        this.fileName = fileName;
        this.builder = builder;
    }

    public String fileName() {
        return fileName;
    }

    public synchronized Path path() {
        if (built == null) {
            try {
                Path dir = Files.createDirectories(Fixtures.root().resolve("generated"));
                Path partial = dir.resolve(fileName + ".partial");
                Files.deleteIfExists(partial);
                try (Database db = Fixtures.create(partial)) {
                    builder.build(db);
                }
                built = Files.move(partial, dir.resolve(fileName), REPLACE_EXISTING, ATOMIC_MOVE);
            } catch (IOException e) {
                throw new UncheckedIOException("building fixture " + fileName, e);
            }
        }
        return built;
    }

    public Database open() throws IOException {
        return Fixtures.openReadOnly(path());
    }

    @FunctionalInterface
    interface Builder {
        void build(Database db) throws IOException;
    }
}
