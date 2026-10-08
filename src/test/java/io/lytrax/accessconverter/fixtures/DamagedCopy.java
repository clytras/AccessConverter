package io.lytrax.accessconverter.fixtures;

import com.healthmarketscience.jackcess.Cursor;
import com.healthmarketscience.jackcess.CursorBuilder;
import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.Row;
import com.healthmarketscience.jackcess.impl.DatabaseImpl;
import com.healthmarketscience.jackcess.impl.IndexData;
import com.healthmarketscience.jackcess.impl.IndexImpl;
import com.healthmarketscience.jackcess.impl.PageChannel;
import com.healthmarketscience.jackcess.impl.RowIdImpl;
import com.healthmarketscience.jackcess.impl.TableImpl;
import com.healthmarketscience.jackcess.impl.UsageMap;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Random;

/**
 * Test helper: a copy of a database with one part damaged, as a disk or a crashed write damages it. The corpus ships
 * no damaged files, so the damage is made where the test needs it, and the rest of the file is left intact: a
 * table's first data page, a table's index, or one entry of the catalog's index.
 */
public final class DamagedCopy {

    /** Jackcess's page type byte for a data page ({@code PageTypes.DATA}). */
    private static final byte DATA_PAGE = 0x01;

    /** Jackcess's page type byte for an index leaf page ({@code PageTypes.INDEX_LEAF}). */
    private static final byte INDEX_LEAF_PAGE = 0x04;

    private DamagedCopy() {}

    /** Copies {@code source} to {@code copy} and damages the first data page of {@code table} in it. */
    public static Path withDataPageDamaged(Path source, String table, Path copy) throws IOException {
        Files.copy(source, copy, StandardCopyOption.REPLACE_EXISTING);
        int pageSize;
        long page = -1;
        try (Database db = Fixtures.openReadOnly(copy)) {
            PageChannel pages = ((DatabaseImpl) db).getPageChannel();
            pageSize = pages.getFormat().PAGE_SIZE;
            UsageMap.PageCursor owned = ((TableImpl) db.getTable(table)).getOwnedPagesCursor();
            ByteBuffer buffer = pages.createPageBuffer();
            for (int number = owned.getNextPage();
                    number != RowIdImpl.LAST_PAGE_NUMBER && page < 0;
                    number = owned.getNextPage()) {
                pages.readPage(buffer, number);
                if (buffer.get(0) == DATA_PAGE) {
                    page = number;
                }
            }
        }
        if (page < 0) {
            throw new IllegalArgumentException(table + " has no data page");
        }
        byte[] garbage = new byte[pageSize];
        new Random(42).nextBytes(garbage);
        garbage[0] = DATA_PAGE; // still a data page, with nonsense for its row offsets and rows
        try (FileChannel file = FileChannel.open(copy, StandardOpenOption.WRITE)) {
            file.write(ByteBuffer.wrap(garbage), page * pageSize);
        }
        return copy;
    }

    /**
     * Copies {@code source} to {@code copy} and damages the root page of {@code table}'s index {@code index}: past its
     * header, most of the page is overwritten with nonsense, so its entries are out of order. The table's rows, which
     * a scan reads, are left intact; a lookup through the index fails.
     */
    public static Path withIndexDamaged(Path source, String table, String index, Path copy) throws IOException {
        Files.copy(source, copy, StandardCopyOption.REPLACE_EXISTING);
        int pageSize;
        int root;
        try (Database db = Fixtures.openReadOnly(copy)) {
            pageSize = ((DatabaseImpl) db).getPageChannel().getFormat().PAGE_SIZE;
            root = rootPage((IndexImpl) db.getTable(table).getIndex(index));
        }
        byte[] garbage = new byte[3000];
        new Random(42).nextBytes(garbage);
        try (FileChannel file = FileChannel.open(copy, StandardOpenOption.WRITE)) {
            file.write(ByteBuffer.wrap(garbage), (long) root * pageSize + 40);
        }
        return copy;
    }

    /**
     * Copies {@code source} to {@code copy} and damages the entry for {@code object} (a table, or a system table such
     * as {@code MSysRelationships}) in the index Jackcess finds catalog rows by ({@code ParentIdName} on {@code
     * MSysObjects}): one bit of the name's key is flipped, so a lookup of that name finds nothing, while the catalog's
     * rows, which a scan reads, are left intact. Encoded files (a stream cipher, RC4) are damaged the same way, since
     * flipping a bit of an encoded byte flips the same bit of the byte it decodes to. Only for a catalog whose index is
     * one leaf page and that Jackcess can use: a collation it can't index (Greek, for one) makes it scan anyway.
     */
    public static Path withCatalogIndexDamaged(Path source, String password, String object, Path copy)
            throws IOException {
        Files.copy(source, copy, StandardCopyOption.REPLACE_EXISTING);
        int pageSize;
        int root;
        int at = -1;
        try (Database db = Fixtures.openReadOnly(copy, password)) {
            DatabaseImpl impl = (DatabaseImpl) db;
            TableImpl catalog = impl.getSystemCatalog();
            IndexImpl index = catalog.getIndexes().stream()
                    .filter(i -> i.getColumns().size() == 2)
                    .findFirst()
                    .orElseThrow(() -> new IllegalArgumentException("no ParentIdName index"));
            root = rootPage(index);
            RowIdImpl row = null;
            Cursor cursor = CursorBuilder.createCursor(catalog);
            for (Row r : cursor) {
                if (object.equals(r.getString("Name"))) {
                    row = (RowIdImpl) cursor.getSavepoint().getCurrentPosition().getRowId();
                    break;
                }
            }
            if (row == null) {
                throw new IllegalArgumentException(object + " is not in the catalog");
            }
            PageChannel pages = impl.getPageChannel();
            pageSize = pages.getFormat().PAGE_SIZE;
            ByteBuffer page = pages.createPageBuffer();
            pages.readPage(page, root);
            if (page.get(0) != INDEX_LEAF_PAGE) {
                throw new IllegalArgumentException("the catalog index is more than one page");
            }
            // An entry ends with its row's page number (3 bytes, big-endian) and row number
            byte[] pointer = {
                (byte) (row.getPageNumber() >> 16),
                (byte) (row.getPageNumber() >> 8),
                (byte) row.getPageNumber(),
                (byte) row.getRowNumber()
            };
            for (int i = 2; i + pointer.length <= pageSize && at < 0; i++) {
                if (page.get(i) == pointer[0]
                        && page.get(i + 1) == pointer[1]
                        && page.get(i + 2) == pointer[2]
                        && page.get(i + 3) == pointer[3]) {
                    at = i - 2; // the last byte of the name's key, before the key's end marker
                }
            }
        }
        if (at < 0) {
            throw new IllegalArgumentException(object + " has no entry in the catalog index");
        }
        try (FileChannel file = FileChannel.open(copy, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            ByteBuffer one = ByteBuffer.allocate(1);
            long offset = (long) root * pageSize + at;
            file.read(one, offset);
            one.put(0, (byte) (one.get(0) ^ 0x01)).rewind();
            file.write(one, offset);
        }
        return copy;
    }

    /** The index's root page: Jackcess keeps it to itself, so it is read by reflection. */
    private static int rootPage(IndexImpl index) {
        try {
            Method method = IndexData.class.getDeclaredMethod("getRootPageNumber");
            method.setAccessible(true);
            return (int) method.invoke(index.getIndexData());
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Jackcess's IndexData has changed", e);
        }
    }
}
