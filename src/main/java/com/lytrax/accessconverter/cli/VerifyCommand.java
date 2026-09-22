package com.lytrax.accessconverter.cli;

import com.lytrax.accessconverter.extract.ExtractOptions;
import com.lytrax.accessconverter.extract.SchemaExtractor;
import com.lytrax.accessconverter.model.SchemaModel;
import com.lytrax.accessconverter.report.Issues;
import com.lytrax.accessconverter.source.AccessSource;
import com.lytrax.accessconverter.verify.SourceSnapshot;
import com.lytrax.accessconverter.verify.SourceSnapshot.TableSnapshot;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.util.concurrent.Callable;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.Spec;

@Command(
        name = "verify",
        mixinStandardHelpOptions = true,
        description = {
            "Compare a converted output with its Access source (09).",
            "Without <output>, print the source side: each table's row count and a digest of its canonical rows."
        })
final class VerifyCommand implements Callable<Integer> {
    @Parameters(index = "0", paramLabel = "<input>", description = "Access database (.mdb, .accdb)")
    Path input;

    @Parameters(index = "1", arity = "0..1", paramLabel = "<output>", description = "Converted output to compare")
    Path output;

    @Spec
    CommandSpec spec;

    @Override
    public Integer call() throws IOException {
        Main.requireFile(input);
        if (output != null) {
            spec.commandLine()
                    .getErr()
                    .println("error: comparing an output needs its target, which isn't implemented yet: " + output);
            return ExitCodes.USAGE;
        }
        Issues issues = new Issues();
        SourceSnapshot snapshot;
        try (AccessSource source = AccessSource.open(input)) {
            SchemaModel model = SchemaExtractor.extract(source, ExtractOptions.ALL, issues);
            snapshot = SourceSnapshot.capture(source, model);
        }
        PrintWriter out = spec.commandLine().getOut();
        StringBuilder text = new StringBuilder();
        text.append("Source: ")
                .append(snapshot.source().fileName())
                .append(" (")
                .append(snapshot.source().fileFormat())
                .append(")\n");
        for (TableSnapshot table : snapshot.tables()) {
            text.append("  ")
                    .append(table.table())
                    .append(": ")
                    .append(table.rows())
                    .append(" rows, sha256 ")
                    .append(table.sha256())
                    .append('\n');
        }
        out.print(text);
        out.flush();
        return ExitCodes.of(issues);
    }
}
