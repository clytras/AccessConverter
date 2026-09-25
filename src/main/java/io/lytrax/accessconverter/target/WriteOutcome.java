package io.lytrax.accessconverter.target;

import io.lytrax.accessconverter.report.ConversionReport.TableResult;
import java.util.List;

/**
 * What a writer produced: each table's rows read and written.
 *
 * @param tableFailed a table could not be written and {@code --on-table-error continue} kept the output
 */
public record WriteOutcome(List<TableResult> tables, boolean tableFailed) {
    public WriteOutcome {
        tables = List.copyOf(tables);
    }

    public long rowsWritten() {
        return tables.stream()
                .mapToLong(t -> t.rowsWritten() == null ? 0 : t.rowsWritten())
                .sum();
    }
}
