package io.lytrax.accessconverter.target;

import io.lytrax.accessconverter.model.TableModel;
import io.lytrax.accessconverter.source.RowStream;
import java.io.IOException;
import java.util.Set;

/**
 * Where a writer's rows come from. The Access source provides them; a test replaces it to make one table fail, which
 * is the only way to exercise {@code --on-table-error} without a database that pretends to be damaged.
 */
@FunctionalInterface
public interface RowSource {
    RowStream of(TableModel table) throws IOException;

    /**
     * The same rows, except for the tables in {@code failed}, which the profiling pass couldn't read and reported
     * (04): those fail at once with {@link TableFailure.AlreadyReported}, without being read again.
     */
    default RowSource skipping(Set<String> failed) {
        return table -> {
            if (failed.contains(table.name())) {
                throw new TableFailure.AlreadyReported(table.name());
            }
            return of(table);
        };
    }
}
