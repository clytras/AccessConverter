package com.lytrax.accessconverter.cli;

import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Writer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.concurrent.Callable;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.IVersionProvider;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Spec;

@Command(
        name = "accessconverter",
        mixinStandardHelpOptions = true,
        versionProvider = Main.Version.class,
        subcommands = {InspectCommand.class, VerifyCommand.class},
        description = "Converts Microsoft Access databases (.mdb, .accdb) to MySQL/MariaDB dumps, SQLite and JSON.",
        exitCodeListHeading = "%nExit codes:%n",
        exitCodeList = {"0:success", "1:success with warnings (see the issues)", "2:failed", "64:usage error"})
public final class Main implements Callable<Integer> {
    private final OutputStream stdout;

    @Spec
    CommandSpec spec;

    private Main(OutputStream stdout) {
        this.stdout = stdout;
    }

    public static void main(String[] args) {
        System.exit(run(System.out, System.out.charset(), new PrintWriter(System.err, true), args));
    }

    /**
     * Runs a command line; returns the exit code. Text goes to {@code stdout} in {@code textCharset} (the
     * console's encoding, so it displays correctly); machine-readable output such as JSON is always UTF-8.
     */
    public static int run(OutputStream stdout, Charset textCharset, PrintWriter err, String... args) {
        PrintWriter out = new PrintWriter(new OutputStreamWriter(stdout, textCharset), true);
        CommandLine cli = new CommandLine(new Main(stdout))
                .setOut(out)
                .setErr(err)
                .setCaseInsensitiveEnumValuesAllowed(true)
                .setExecutionExceptionHandler((e, commandLine, parseResult) -> {
                    commandLine.getErr().println("error: " + message(e));
                    return ExitCodes.FAILED;
                });
        cli.getCommandSpec().exitCodeOnInvalidInput(ExitCodes.USAGE);
        cli.getSubcommands().values().forEach(sub -> sub.getCommandSpec().exitCodeOnInvalidInput(ExitCodes.USAGE));
        int code = cli.execute(args);
        out.flush();
        return code;
    }

    /** Without a subcommand: print usage, which is a usage error. */
    @Override
    public Integer call() {
        spec.commandLine().getErr().println("error: a command is required");
        spec.commandLine().usage(spec.commandLine().getErr());
        return ExitCodes.USAGE;
    }

    /** Standard output for machine-readable results: UTF-8 whatever the console's code page. */
    Writer utf8Out() {
        return new OutputStreamWriter(stdout, StandardCharsets.UTF_8);
    }

    static void requireFile(Path file) throws NoSuchFileException {
        if (!Files.isRegularFile(file)) {
            throw new NoSuchFileException(file.toString(), null, "no such file");
        }
    }

    private static String message(Exception e) {
        String message = e.getMessage();
        return message == null ? e.getClass().getSimpleName() : e.getClass().getSimpleName() + ": " + message;
    }

    static final class Version implements IVersionProvider {
        @Override
        public String[] getVersion() {
            return new String[] {"accessconverter " + version()};
        }

        static String version() {
            String version = Main.class.getPackage().getImplementationVersion();
            return version == null ? "dev" : version;
        }
    }
}
