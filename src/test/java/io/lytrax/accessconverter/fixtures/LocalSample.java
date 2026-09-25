package io.lytrax.accessconverter.fixtures;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.healthmarketscience.jackcess.Database;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

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
    HOTEL_MANAGEMENT_SYSTEM("test/HotelManagementSystem.accdb"),
    /** Access 97 from the maintainer's Windows 98 guest, Greek Office: the catalog index Jackcess can't use. */
    NORTHWIND_97("samples/win98/Northwind.mdb", "samples/win98/northwind.dump.txt"),
    SOLUTIONS_97("samples/win98/Solutions.mdb"),
    ORDERS_97("samples/win98/ORDERS.MDB");

    private final String relativePath;
    private final String dumpPath;

    LocalSample(String relativePath) {
        this(relativePath, null);
    }

    LocalSample(String relativePath, String dumpPath) {
        this.relativePath = relativePath;
        this.dumpPath = dumpPath;
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

    /** What Access itself printed for this database in the Windows 98 guest, where there is such a dump. */
    public GuestDump dump() {
        Path file = root().resolve(Objects.requireNonNull(dumpPath, () -> name() + " has no dump"));
        assumeTrue(Files.isRegularFile(file), () -> "dump not present: " + file);
        return GuestDump.read(file);
    }

    /** The repository root when run by Maven ({@code -Dsamples.root}), else the working directory. */
    private static Path root() {
        String root = System.getProperty("samples.root");
        return (root != null ? Path.of(root) : Path.of("")).toAbsolutePath();
    }
}
