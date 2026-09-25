package io.lytrax.accessconverter.report;

import static io.lytrax.accessconverter.report.JsonSupport.optional;

import io.lytrax.accessconverter.model.SchemaModel;
import java.io.Writer;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;
import tools.jackson.core.JsonGenerator;

/**
 * The machine-readable result of a command (03, The conversion report): what ran, on what, what was written, and
 * every issue. It replaces v2's {@code *.log.json} and is always written, even when the conversion fails.
 *
 * <p>Everything except {@code timings} is deterministic for the same input and options.
 *
 * @param options the effective options, by name
 * @param tables per-table results, in model order
 * @param outcome {@code success}, {@code success-with-warnings} or {@code failed}
 */
public record ConversionReport(
        String toolVersion,
        String command,
        SortedMap<String, String> options,
        SchemaModel.Source source,
        List<TableResult> tables,
        List<Issue> issues,
        String outcome,
        Map<String, Duration> timings) {

    public static final int FORMAT_VERSION = 1;

    public ConversionReport {
        Objects.requireNonNull(command, "command");
        options = new TreeMap<>(options);
        tables = List.copyOf(tables);
        issues = List.copyOf(issues);
        timings = Map.copyOf(timings);
    }

    /** @param rowsRead rows read from Access; {@code rowsWritten} is null until a target writes them */
    public record TableResult(String table, long rowsRead, Long rowsWritten) {}

    public void write(Writer out) {
        try (JsonGenerator g = JsonSupport.pretty(out)) {
            g.writeStartObject();
            g.writeStringProperty("format", "accessconverter-report");
            g.writeNumberProperty("formatVersion", FORMAT_VERSION);
            g.writeStringProperty("tool", "accessconverter " + toolVersion);
            g.writeStringProperty("command", command);
            g.writeObjectPropertyStart("options");
            options.forEach(g::writeStringProperty);
            g.writeEndObject();
            g.writeObjectPropertyStart("source");
            g.writeStringProperty("file", source.fileName());
            g.writeStringProperty("fileFormat", source.fileFormat());
            optional(g, "codePage", source.codePage());
            g.writeStringProperty("charset", source.charset());
            g.writeEndObject();
            g.writeStringProperty("outcome", outcome);
            g.writeArrayPropertyStart("tables");
            for (TableResult t : tables) {
                g.writeStartObject();
                g.writeStringProperty("table", t.table());
                g.writeNumberProperty("rowsRead", t.rowsRead());
                optional(g, "rowsWritten", t.rowsWritten());
                g.writeEndObject();
            }
            g.writeEndArray();
            ModelJson.issues(g, issues);
            g.writeObjectPropertyStart("timings");
            new TreeMap<>(timings).forEach((stage, d) -> g.writeNumberProperty(stage + "Millis", d.toMillis()));
            g.writeEndObject();
            g.writeEndObject();
        }
    }
}
