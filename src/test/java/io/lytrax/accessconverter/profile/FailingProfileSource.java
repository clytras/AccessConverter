package io.lytrax.accessconverter.profile;

import io.lytrax.accessconverter.model.ColumnModel;
import io.lytrax.accessconverter.model.IndexModel;
import io.lytrax.accessconverter.model.TableModel;
import io.lytrax.accessconverter.source.AccessSource;
import io.lytrax.accessconverter.source.KeyLookup;
import io.lytrax.accessconverter.source.RowStream;
import io.lytrax.accessconverter.source.SourceException;
import java.io.IOException;
import java.util.List;

/**
 * Test helper: the profiling pass's reads from the real database, except one table, whose scans and key lookups fail
 * as a damaged table's do. The failure is injected here, as the writers' tests inject it at their row source.
 */
public record FailingProfileSource(AccessSource source, String table) implements ProfileSource {

    @Override
    public RowStream scan(TableModel model, List<ColumnModel> columns) throws IOException {
        fail(model);
        return source.scan(model, columns);
    }

    @Override
    public KeyLookup keyLookup(TableModel model, IndexModel key) throws IOException {
        fail(model);
        return source.keyLookup(model, key);
    }

    private void fail(TableModel model) throws SourceException {
        if (model.name().equalsIgnoreCase(table)) {
            throw SourceException.readFailed(
                    source.file(), "table " + model.name(), new IOException("page 42 is damaged"));
        }
    }
}
