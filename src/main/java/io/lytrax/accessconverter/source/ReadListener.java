package io.lytrax.accessconverter.source;

import io.lytrax.accessconverter.model.TableModel;

/**
 * Told when {@link AccessSource} starts a pass over a table's rows, and of each row that pass reads; for progress on
 * the console. It only observes: nothing it does can change what is read or written.
 */
@FunctionalInterface
public interface ReadListener {
    /** Listens to nothing. */
    ReadListener NONE = table -> () -> {};

    /**
     * A pass over {@code table}'s rows begins. Passes can nest (the profiler may read a parent's keys while it reads
     * a child), so each one counts its rows on its own.
     *
     * @return run once for each row this pass reads
     */
    Runnable started(TableModel table);
}
