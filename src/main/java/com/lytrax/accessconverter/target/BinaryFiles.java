package com.lytrax.accessconverter.target;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;

import com.lytrax.accessconverter.report.IssueCode;
import com.lytrax.accessconverter.report.Issues;
import com.lytrax.accessconverter.value.MimeSniffer;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashMap;
import java.util.Map;

/**
 * {@code --binary files} (08): every Binary, OLE and attachment value becomes a file under {@code <output>-files/}, at
 * {@code <table>/<column>/<rowkey>[-<name>]}, and the output holds its path relative to the output's own directory,
 * with forward slashes on every OS.
 *
 * <p>Names come from data (attachment and package names, table and column names, key values), so every part is made
 * safe by {@link FileNames} (no directory parts, no reserved characters or device names, at most 200 characters), a
 * name that collides after that gets {@code -<n>}, a file is never overwritten, and every final path is resolved and
 * must stay inside the files directory.
 *
 * <p>The directory is built as {@code <output>-files.partial/} and renamed into place by {@link #commit()} once the
 * output itself is in place, like the output ({@link AtomicOutput}); {@link #discard()} removes it after a failure. It
 * is only created when a file is written.
 */
public final class BinaryFiles {

    private static final String SUFFIX = "-files";

    private final Path directory;
    private final Path partial;
    private final FileNames tables = new FileNames();
    private final Map<String, Table> byTable = new HashMap<>();
    private boolean created;

    public BinaryFiles(Path output) throws IOException {
        this.directory = directoryOf(output);
        this.partial = directory.resolveSibling(directory.getFileName() + ".partial");
        deleteTree(partial);
    }

    /** {@code <output>-files}, next to the output. */
    public static Path directoryOf(Path output) {
        Path absolute = output.toAbsolutePath().normalize();
        return absolute.resolveSibling(absolute.getFileName() + SUFFIX);
    }

    /**
     * Writes one value to a new file and returns its path relative to the output's directory.
     *
     * @param table the table as the output names it
     * @param column the column as the output names it
     * @param stem the row key, with {@code -<n>} for the n-th file of a cell with several
     * @param name the data's own file name (an attachment's, a package's), or null: then the file is named after the
     *     stem, with an extension from the content's magic number or {@code .bin}
     */
    public String write(String table, String column, String stem, String name, byte[] bytes) throws IOException {
        Table dir = byTable.computeIfAbsent(table, t -> new Table(tables.allocate(t, "")));
        Column columnDir = dir.columns.computeIfAbsent(column, c -> new Column(dir.columnNames.allocate(c, "")));
        String fileName;
        if (name == null || name.isBlank()) {
            String extension = MimeSniffer.extension(MimeSniffer.sniff(bytes));
            fileName = columnDir.files.allocate(stem, extension == null ? ".bin" : extension);
        } else {
            String safe = FileNames.sanitize(name, FileNames.MAX_LENGTH);
            int dot = safe.lastIndexOf('.');
            boolean hasExtension = dot > 0 && safe.length() - dot <= 16;
            String base = stem + "-" + (hasExtension ? safe.substring(0, dot) : safe);
            fileName = columnDir.files.allocate(base, hasExtension ? safe.substring(dot) : "");
        }
        if (!created) {
            Files.createDirectory(partial);
            created = true;
        }
        Path root = partial.toAbsolutePath().normalize();
        Path file =
                root.resolve(dir.name).resolve(columnDir.name).resolve(fileName).normalize();
        if (!file.startsWith(root) || file.getNameCount() != root.getNameCount() + 3) {
            // Unreachable with sanitized names; kept so no name can ever write outside the directory
            throw new IOException("the file name " + fileName + " would be written outside " + directory);
        }
        Files.createDirectories(file.getParent());
        Files.write(file, bytes, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
        return directory.getFileName() + "/" + dir.name + "/" + columnDir.name + "/" + fileName;
    }

    /**
     * Puts the files in place, replacing an earlier {@code <output>-files/} (the output was replaced too, so its files
     * go with it). Without a file to write, an earlier directory is removed and none is created.
     */
    public void commit() throws IOException {
        deleteTree(directory);
        if (created) {
            Files.move(partial, directory, ATOMIC_MOVE);
        }
    }

    /**
     * Removes the files of a table that failed and is left empty (03, Error handling), so no bytes stay on disk for rows
     * the output doesn't have. A table's files are all under its own directory. What can't be removed is reported,
     * never left behind quietly.
     *
     * @param table the table as the output names it, as passed to {@link #write}
     */
    public void discardTable(String table, Issues issues) {
        Table dir = byTable.remove(table);
        if (dir == null) {
            return;
        }
        Path files = partial.resolve(dir.name);
        try {
            deleteTree(files);
        } catch (IOException e) {
            issues.add(
                    IssueCode.BINARY_FILES_NOT_REMOVED,
                    table,
                    null,
                    "the table failed and is empty, but its files under " + directory.getFileName() + "/" + dir.name
                            + " could not all be removed: " + e.getMessage());
        }
    }

    /** Removes what was written; after a failed conversion. */
    public void discard() throws IOException {
        deleteTree(partial);
    }

    /**
     * Deletes a directory tree this class wrote: directories and regular files only. A symbolic link anywhere means it
     * isn't one, and nothing is deleted.
     */
    static void deleteTree(Path root) throws IOException {
        if (!Files.exists(root, LinkOption.NOFOLLOW_LINKS)) {
            return;
        }
        if (!Files.isDirectory(root, LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException(root + " exists and is not a directory of extracted files; it won't be replaced");
        }
        try (var walk = Files.walk(root)) {
            for (Path p : (Iterable<Path>) walk::iterator) {
                if (Files.isSymbolicLink(p)
                        || !(Files.isRegularFile(p, LinkOption.NOFOLLOW_LINKS)
                                || Files.isDirectory(p, LinkOption.NOFOLLOW_LINKS))) {
                    throw new IOException(root + " holds " + root.relativize(p)
                            + ", which this conversion doesn't write; it won't be replaced");
                }
            }
        }
        Files.walkFileTree(root, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException e) throws IOException {
                if (e != null) {
                    throw e;
                }
                Files.delete(dir);
                return FileVisitResult.CONTINUE;
            }
        });
    }

    private static final class Table {
        final String name;
        final FileNames columnNames = new FileNames();
        final Map<String, Column> columns = new HashMap<>();

        Table(String name) {
            this.name = name;
        }
    }

    private static final class Column {
        final String name;
        final FileNames files = new FileNames();

        Column(String name) {
            this.name = name;
        }
    }
}
