package io.lytrax.accessconverter.profile;

import io.lytrax.accessconverter.model.ColumnModel;
import io.lytrax.accessconverter.model.IndexModel;
import io.lytrax.accessconverter.model.TableModel;
import io.lytrax.accessconverter.source.AccessSource;
import io.lytrax.accessconverter.source.KeyLookup;
import io.lytrax.accessconverter.source.RowStream;
import java.io.IOException;
import java.util.List;

/**
 * What the profiling pass reads: the Access source's targeted scans and key lookups. A test replaces it to make one
 * table fail, as {@code RowSource} does for the writers.
 */
public interface ProfileSource {

    /** As {@link AccessSource#scan}. */
    RowStream scan(TableModel table, List<ColumnModel> columns) throws IOException;

    /** As {@link AccessSource#keyLookup}. */
    KeyLookup keyLookup(TableModel table, IndexModel key) throws IOException;

    static ProfileSource of(AccessSource source) {
        return new ProfileSource() {
            @Override
            public RowStream scan(TableModel table, List<ColumnModel> columns) throws IOException {
                return source.scan(table, columns);
            }

            @Override
            public KeyLookup keyLookup(TableModel table, IndexModel key) throws IOException {
                return source.keyLookup(table, key);
            }
        };
    }
}
