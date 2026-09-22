package com.lytrax.accessconverter.fixtures;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.healthmarketscience.jackcess.Database;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Tier C: the maintainer's real-world databases in the gitignored {@code samples/} and {@code test/} directories.
 * Their licenses are unclear, so they're never committed. Tests using them carry {@link LocalSamples}, run only
 * with {@code -Plocal-samples}, and are skipped (not failed) when a file is absent.
 */
public enum LocalSample {
    NORTHWIND_2007("samples/Northwind-2007.accdb"),
    ALL_TYPES("samples/allTypes.accdb"),
    ALL_TYPES_MDB("samples/allTypes_mdb.accdb"),
    TEST_DB("samples/testDB.mdb"),
    MARKET_BASKET("samples/MarketBasket.accdb"),
    FAV_DATABASE("test/FavDatabase.mdb"),
    HOTEL_MANAGEMENT_SYSTEM("test/HotelManagementSystem.accdb");

    private final String relativePath;

    LocalSample(String relativePath) {
        this.relativePath = relativePath;
    }

    /** The sample's path; aborts the calling test (reported as skipped) when the file isn't there. */
    public Path path() {
        Path file = root().resolve(relativePath);
        assumeTrue(Files.isRegularFile(file), () -> "local sample not present: " + file);
        return file;
    }

    public Database open() throws IOException {
        return Fixtures.openReadOnly(path());
    }

    /** The repository root when run by Maven ({@code -Dsamples.root}), else the working directory. */
    private static Path root() {
        String root = System.getProperty("samples.root");
        return (root != null ? Path.of(root) : Path.of("")).toAbsolutePath();
    }
}
