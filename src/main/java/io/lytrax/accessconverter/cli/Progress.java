package io.lytrax.accessconverter.cli;

import io.lytrax.accessconverter.model.TableModel;
import io.lytrax.accessconverter.source.ReadListener;
import java.io.PrintWriter;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/**
 * Progress on standard error: one line, redrawn in place and erased at the end, such as {@code write 3/21 Orders
 * 45,000 / 120,000 rows 37%}. Standard output is never touched (it may carry {@code --format-result json}), and
 * nothing here reaches an output file or the report.
 *
 * <p>A redirected standard error can't be detected (02), so the line has to stay cheap in a log: nothing is drawn in
 * the first half second, at most five times a second after that, and only when what it shows changes: the stage, the
 * table, or the percentage (the row count when Access gives no total). A table then costs at most about a hundred
 * redraws, however many rows it has.
 */
final class Progress implements ReadListener, AutoCloseable {

    static final long QUIET_NANOS = TimeUnit.MILLISECONDS.toNanos(500);
    static final long INTERVAL_NANOS = TimeUnit.MILLISECONDS.toNanos(200);

    /** The widest line drawn, so it fits an 80-column console without wrapping. */
    static final int WIDTH = 79;

    /** Rows between two looks at the clock once a table has been drawn. */
    private static final int ROWS_PER_CHECK = 64;

    private final PrintWriter err;
    private final LongSupplier clock;
    private final long quietUntil;
    private final Map<String, Integer> positions = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    private String stage = "";
    private Pass current;
    private boolean changed;
    private long nextDraw;
    private String shown = "";
    private int width;

    private Progress(PrintWriter err, LongSupplier clock) {
        this.err = err;
        this.clock = clock;
        this.quietUntil = clock.getAsLong() + QUIET_NANOS;
    }

    /** Progress drawn on {@code err}, timed by {@code clock} (nanoseconds, as {@link System#nanoTime()}). */
    static Progress on(PrintWriter err, LongSupplier clock) {
        return new Progress(err, clock);
    }

    /** No progress: every call does nothing, and the source isn't observed at all. */
    static Progress off() {
        return new Progress(null, () -> 0);
    }

    boolean enabled() {
        return err != null;
    }

    /** What to give {@code AccessSource.listen}: this, or nothing when progress is off. */
    ReadListener listener() {
        return enabled() ? this : ReadListener.NONE;
    }

    /** A stage that reads the rows of {@code tables} (linked ones have none and are left out of the count). */
    void stage(String name, List<TableModel> tables) {
        if (!enabled()) {
            return;
        }
        stage = name;
        positions.clear();
        for (TableModel table : tables) {
            if (!table.isLinked()) {
                positions.putIfAbsent(table.name(), positions.size() + 1);
            }
        }
        current = null;
    }

    @Override
    public Runnable started(TableModel table) {
        Pass pass = new Pass(table.name(), table.isComplexChild() ? -1 : table.rowCount());
        current = pass;
        changed = true;
        draw();
        return pass;
    }

    /** Erases the line, if one was drawn. */
    @Override
    public void close() {
        if (enabled() && width > 0) {
            err.print("\r" + " ".repeat(width) + "\r");
            err.flush();
            width = 0;
            shown = "";
        }
    }

    private void draw() {
        long now = clock.getAsLong();
        if (now - quietUntil < 0 || now - nextDraw < 0) {
            return;
        }
        changed = false;
        String key = key(current);
        if (key.equals(shown)) {
            return;
        }
        nextDraw = now + INTERVAL_NANOS;
        shown = key;
        String line = line(current);
        String padding = " ".repeat(Math.max(0, width - line.length()));
        err.print("\r" + line + padding);
        err.flush();
        width = line.length();
    }

    /** What decides a redraw: the stage, the table and the percentage, or the count when there is no total. */
    private String key(Pass pass) {
        return stage + '\n' + pass.table + '\n' + (pass.total < 0 ? pass.rows : percent(pass.rows, pass.total));
    }

    String line(Pass pass) {
        Integer position = positions.get(pass.table);
        String prefix = stage + (position == null ? "" : " " + position + "/" + positions.size()) + " ";
        String counts = pass.total < 0
                ? String.format(Locale.ROOT, pass.rows == 1 ? "%,d row" : "%,d rows", pass.rows)
                : String.format(
                        Locale.ROOT, "%,d / %,d rows  %d%%", pass.rows, pass.total, percent(pass.rows, pass.total));
        int room = WIDTH - prefix.length() - 2 - counts.length();
        return prefix + fit(pass.table, room) + "  " + counts;
    }

    private static int percent(long rows, long total) {
        return total <= 0 ? 100 : (int) Math.min(100, rows * 100 / total);
    }

    /** {@code name}, shortened with "..." to at most {@code room} characters (never splitting a surrogate pair). */
    static String fit(String name, int room) {
        if (name.length() <= room) {
            return name;
        }
        int keep = Math.max(1, room - 3);
        int end = name.offsetByCodePoints(0, Math.min(keep, name.codePointCount(0, name.length())));
        while (end > keep && end > 1) {
            end = name.offsetByCodePoints(end, -1);
        }
        return name.substring(0, end) + "...";
    }

    /** One pass over a table's rows, counting them. */
    final class Pass implements Runnable {
        final String table;
        final long total;
        long rows;

        Pass(String table, long total) {
            this.table = table;
            this.total = total;
        }

        @Override
        public void run() {
            rows++;
            if (current != this) {
                // A nested pass (a parent's keys) finished: this one is being read again
                current = this;
                changed = true;
            }
            if (changed || rows % ROWS_PER_CHECK == 0 || rows == total) {
                draw();
            }
        }
    }
}
