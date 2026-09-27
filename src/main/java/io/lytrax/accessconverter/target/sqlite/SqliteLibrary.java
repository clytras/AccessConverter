package io.lytrax.accessconverter.target.sqlite;

import io.lytrax.accessconverter.target.OutputException;
import java.nio.file.Path;
import org.sqlite.SQLiteJDBCLoader;
import org.sqlite.util.LibraryLoaderUtil;

/**
 * sqlite-jdbc's native library, loaded before the first connection: sqlite-jdbc reports a library it can't load and a
 * file it can't open alike ({@code Error opening connection}), and only the first is fixed by the user's JVM options.
 *
 * <p>sqlite-jdbc loads the library from {@code org.sqlite.lib.path} when that is set (the container, and runtime
 * images that hold the library), else unpacks it from the jar into {@code org.sqlite.tmpdir} or
 * {@code java.io.tmpdir} and loads it from there. The message names what it tried.
 */
final class SqliteLibrary {

    private SqliteLibrary() {}

    /** Loads the library, or fails with why it can't be loaded, under the name of the SQLite {@code file} at hand. */
    static void load(Path file) throws OutputException {
        try {
            SQLiteJDBCLoader.initialize();
        } catch (Exception | LinkageError e) {
            throw new OutputException(file, reason(), e);
        }
    }

    static String reason() {
        String folderProperty =
                System.getProperty("org.sqlite.tmpdir") != null ? "org.sqlite.tmpdir" : "java.io.tmpdir";
        Path folder = Path.of(System.getProperty(folderProperty, "")).toAbsolutePath();
        String unpackable = "make " + folderProperty + " a writable folder that allows running programs";
        String libraryPath = System.getProperty("org.sqlite.lib.path");
        if (libraryPath != null) {
            String name = System.getProperty("org.sqlite.lib.name", LibraryLoaderUtil.getNativeLibName());
            return "the SQLite library could not be loaded from " + Path.of(libraryPath, name) + ", nor unpacked into "
                    + folder + ": the installation may be damaged or for another platform; reinstall it, or "
                    + unpackable;
        }
        if (!LibraryLoaderUtil.hasNativeLib(
                LibraryLoaderUtil.getNativeLibResourcePath(), LibraryLoaderUtil.getNativeLibName())) {
            return "sqlite-jdbc has no SQLite library for this platform (" + System.getProperty("os.name") + ", "
                    + System.getProperty("os.arch") + ")";
        }
        return "the SQLite library could not be unpacked into " + folder + " and loaded from there: " + unpackable
                + " (java -D" + folderProperty + "=<folder>, or ACCESSCONVERTER_JAVA_OPTS=-D" + folderProperty
                + "=<folder> with the runtime image)";
    }
}
