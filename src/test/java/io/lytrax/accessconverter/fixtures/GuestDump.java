package io.lytrax.accessconverter.fixtures;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Ground truth for an Access 97 database, dumped by Access itself in the maintainer's Windows 98 guest
 * ({@code win98 dumpdb}, UTF-8). What it is compared against is the converter's own reading of the same file, so
 * a decoding or catalog bug shows up as a difference. The format:
 *
 * <pre>
 * TABLE Customers
 *   field CustomerID : Long(4)
 *   field Notes : Memo allows-''
 *   index PrimaryKey PRIMARY UNIQUE
 *   | 1	Αφοι Παπαδοπούλου ΑΕ	&lt;NULL&gt;
 *   rows: 8
 * RELATION CustomersOrders: Customers -&gt; Orders on CustomerID=CustomerID
 * </pre>
 *
 * Values are tab-separated, with {@code <NULL>} for NULL, {@code <empty>} for the zero-length string,
 * {@code <binary N bytes>} for OLE, {@code \t}, {@code \r} and {@code \n} for the characters of the same name
 * (a backslash itself is written as it is, so it is only unambiguous when no escape follows), and a trailing
 * {@code ...} where the dumper cut a long value short.
 */
public record GuestDump(List<DumpTable> tables, List<DumpRelation> relations) {
    public static final String NULL = "<NULL>";
    public static final String EMPTY = "<empty>";
    public static final String TRUNCATED = "...";

    /** @param rowCount what the dumper counted, which is not the number of rows it printed */
    public record DumpTable(
            String name, List<String> fields, List<String> indexes, List<List<String>> rows, int rowCount) {
        public List<String> column(String field) {
            int at = fields.indexOf(field);
            if (at < 0) {
                throw new AssertionError("no field " + field + " in " + name);
            }
            return rows.stream().map(row -> row.get(at)).toList();
        }
    }

    public record DumpRelation(String name, String parent, String child, String parentColumn, String childColumn) {}

    public DumpTable table(String name) {
        return tables.stream()
                .filter(t -> t.name().equals(name))
                .findFirst()
                .orElseThrow(() -> new AssertionError("no table " + name + " in the dump"));
    }

    public Optional<DumpRelation> relation(String name) {
        return relations.stream().filter(r -> r.name().equals(name)).findFirst();
    }

    /** The value as Access holds it: escapes undone, and null where the dumper printed {@code <NULL>}. */
    public static String value(String cell) {
        if (NULL.equals(cell)) {
            return null;
        }
        if (EMPTY.equals(cell)) {
            return "";
        }
        return cell.replace("\\r\\n", "\r\n")
                .replace("\\t", "\t")
                .replace("\\r", "\r")
                .replace("\\n", "\n");
    }

    public static GuestDump read(Path file) {
        List<DumpTable> tables = new ArrayList<>();
        List<DumpRelation> relations = new ArrayList<>();
        String name = null;
        List<String> fields = new ArrayList<>();
        List<String> indexes = new ArrayList<>();
        List<List<String>> rows = new ArrayList<>();
        try {
            for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
                if (line.startsWith("TABLE ")) {
                    name = line.substring("TABLE ".length()).trim();
                    fields = new ArrayList<>();
                    indexes = new ArrayList<>();
                    rows = new ArrayList<>();
                } else if (line.startsWith("  field ")) {
                    fields.add(line.substring("  field ".length())
                            .split(" : ", 2)[0]
                            .trim());
                } else if (line.startsWith("  index ")) {
                    indexes.add(line.substring("  index ".length()).trim());
                } else if (line.startsWith("  | ")) {
                    rows.add(List.of(line.substring("  | ".length()).split("\t", -1)));
                } else if (line.startsWith("  rows: ")) {
                    int count =
                            Integer.parseInt(line.substring("  rows: ".length()).trim());
                    tables.add(
                            new DumpTable(name, List.copyOf(fields), List.copyOf(indexes), List.copyOf(rows), count));
                } else if (line.startsWith("RELATION ")) {
                    relations.add(parseRelation(line.substring("RELATION ".length())));
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return new GuestDump(List.copyOf(tables), List.copyOf(relations));
    }

    /** {@code CustomersOrders: Customers -> Orders on CustomerID=CustomerID} */
    private static DumpRelation parseRelation(String text) {
        String[] nameAndRest = text.split(": ", 2);
        String[] tablesAndColumns = nameAndRest[1].split(" on ", 2);
        String[] sides = tablesAndColumns[0].split(" -> ", 2);
        String[] columns = tablesAndColumns[1].trim().split("=", 2);
        return new DumpRelation(nameAndRest[0].trim(), sides[0].trim(), sides[1].trim(), columns[0], columns[1]);
    }
}
