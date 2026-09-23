package com.lytrax.accessconverter.target;

import com.lytrax.accessconverter.model.TableModel;
import com.lytrax.accessconverter.source.RowStream;
import java.io.IOException;

/**
 * Where a writer's rows come from. The Access source provides them; a test replaces it to make one table fail, which
 * is the only way to exercise {@code --on-table-error} without a database that pretends to be damaged.
 */
@FunctionalInterface
public interface RowSource {
    RowStream of(TableModel table) throws IOException;
}
