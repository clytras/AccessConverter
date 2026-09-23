package com.lytrax.accessconverter.cli;

import com.lytrax.accessconverter.extract.ExtractOptions;
import com.lytrax.accessconverter.extract.SchemaExtractor;
import com.lytrax.accessconverter.model.SchemaModel;
import com.lytrax.accessconverter.profile.DataProfile;
import com.lytrax.accessconverter.profile.DataProfiler;
import com.lytrax.accessconverter.report.Issues;
import com.lytrax.accessconverter.report.ModelJson;
import com.lytrax.accessconverter.source.AccessSource;
import com.lytrax.accessconverter.source.OpenOptions;
import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Callable;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.ParentCommand;
import picocli.CommandLine.Spec;

@Command(
        name = "inspect",
        mixinStandardHelpOptions = true,
        description = {
            "Print the normalized schema model of an Access database: tables, columns, keys, indexes (with the"
                    + " hidden ones Access adds folded in), relationships and the issues found."
        })
final class InspectCommand implements Callable<Integer> {
    enum Format {
        text,
        json
    }

    @Parameters(index = "0", paramLabel = "<input>", description = "Access database (.mdb, .accdb)")
    Path input;

    @Option(names = "--profile", description = "Also read the data and print the profiling statistics.")
    boolean profile;

    @Option(
            names = "--format",
            defaultValue = "text",
            description = "Output format: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE}).")
    Format format;

    @Option(
            names = {"-o", "--output"},
            paramLabel = "<file>",
            description = "Write to this file (UTF-8) instead of standard output.")
    Path output;

    @Mixin
    SourceOptions source;

    @Spec
    CommandSpec spec;

    @ParentCommand
    Main main;

    @Override
    public Integer call() throws IOException {
        Main.requireFile(input);
        OpenOptions options = source.toOpenOptions();
        Issues issues = new Issues();
        SchemaModel model;
        DataProfile data = null;
        try (AccessSource db = AccessSource.open(input, options, issues)) {
            model = SchemaExtractor.extract(db, ExtractOptions.ALL, issues);
            if (profile) {
                data = DataProfiler.profile(db, model);
            }
        }
        if (output == null) {
            Writer out =
                    format == Format.json ? main.utf8Out() : spec.commandLine().getOut();
            render(model, data, issues, out);
            out.flush();
        } else {
            try (Writer out = Files.newBufferedWriter(output, StandardCharsets.UTF_8)) {
                render(model, data, issues, out);
            }
        }
        return ExitCodes.of(issues);
    }

    private void render(SchemaModel model, DataProfile data, Issues issues, Writer out) throws IOException {
        if (format == Format.json) {
            ModelJson.write(model, data, issues.list(), out);
            out.write('\n');
        } else {
            out.write(ModelText.render(model, data, issues.list()));
        }
    }
}
