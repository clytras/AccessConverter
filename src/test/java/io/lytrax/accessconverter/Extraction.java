package io.lytrax.accessconverter;

import io.lytrax.accessconverter.extract.ExtractOptions;
import io.lytrax.accessconverter.extract.SchemaExtractor;
import io.lytrax.accessconverter.fixtures.CorpusFile;
import io.lytrax.accessconverter.model.ColumnModel;
import io.lytrax.accessconverter.model.ForeignKeyModel;
import io.lytrax.accessconverter.model.SchemaModel;
import io.lytrax.accessconverter.model.TableModel;
import io.lytrax.accessconverter.report.Issue;
import io.lytrax.accessconverter.report.IssueCode;
import io.lytrax.accessconverter.report.Issues;
import io.lytrax.accessconverter.source.AccessSource;
import io.lytrax.accessconverter.source.OpenOptions;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.List;

/** Test helper: the extracted model of a fixture, with its issues. */
public record Extraction(SchemaModel model, List<Issue> issues) {

    public static Extraction of(Path file) {
        return of(file, OpenOptions.DEFAULT);
    }

    public static Extraction of(CorpusFile file) {
        return of(file.file(), file.openOptions());
    }

    public static Extraction of(Path file, OpenOptions options) {
        Issues issues = new Issues();
        try (AccessSource source = AccessSource.open(file, options, issues)) {
            SchemaModel model = SchemaExtractor.extract(source, ExtractOptions.ALL, issues);
            return new Extraction(model, issues.list());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public TableModel table(String name) {
        return model.table(name).orElseThrow(() -> new AssertionError("no table " + name));
    }

    public ColumnModel column(String table, String column) {
        return table(table).column(column).orElseThrow(() -> new AssertionError("no column " + table + "." + column));
    }

    public ForeignKeyModel relationship(String name) {
        return model.relationships().stream()
                .filter(r -> r.name().equals(name))
                .findFirst()
                .orElseThrow(() -> new AssertionError("no relationship " + name));
    }

    public List<Issue> issues(IssueCode code) {
        return issues.stream().filter(i -> i.code() == code).toList();
    }
}
