package io.lytrax.accessconverter.fixtures;

import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.impl.DatabaseImpl;
import com.healthmarketscience.jackcess.impl.PageChannel;
import com.healthmarketscience.jackcess.impl.RowIdImpl;
import com.healthmarketscience.jackcess.impl.TableImpl;
import com.healthmarketscience.jackcess.impl.UsageMap;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Random;

/**
 * Test helper: a copy of a database with one table's data damaged, as a disk or a crashed write damages it. The
 * corpus ships no damaged files, so the damage is made where the test needs it: the table's first data page is
 * overwritten with random bytes (from a fixed seed, so every run sees the same damage), and the rest of the file,
 * the catalog and the table's definition included, is left intact.
 */
public final class DamagedCopy {

    /** Jackcess's page type byte for a data page ({@code PageTypes.DATA}). */
    private static final byte DATA_PAGE = 0x01;

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
}
