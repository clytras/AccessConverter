package io.lytrax.accessconverter.cli;

import io.lytrax.accessconverter.source.SourceException;
import java.io.BufferedReader;
import java.io.Console;
import java.io.IOException;
import java.io.InputStreamReader;
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
import java.util.logging.Level;
import java.util.logging.Logger;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.IVersionProvider;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.ScopeType;
import picocli.CommandLine.Spec;

@Command(
        name = "accessconverter",
        mixinStandardHelpOptions = true,
        versionProvider = Main.Version.class,
        subcommands = {ConvertCommand.class, InspectCommand.class, VerifyCommand.class},
        description = "Converts Microsoft Access databases (.mdb, .accdb) to SQLite, MySQL/MariaDB dumps and JSON.",
        exitCodeListHeading = "%nExit codes:%n",
        exitCodeList = {"0:success", "1:success with warnings (see the issues)", "2:failed", "64:usage error"})
public final class Main implements Callable<Integer> {
    /** Jackcess logs through System.Logger (JUL by default); held here so the level setting isn't collected. */
    private static final Logger JACKCESS_LOG = Logger.getLogger("com.healthmarketscience");

    private final OutputStream stdout;
    private final PasswordReader passwordReader;
    private final Terminal terminal;

    @Spec
    CommandSpec spec;

    @Option(
            names = {"-v", "--verbose"},
            scope = ScopeType.INHERIT,
            description = "Log Jackcess warnings, and print stack traces on errors.")
    boolean verbose;

    private Main(OutputStream stdout, PasswordReader passwordReader, Terminal terminal) {
        this.stdout = stdout;
        this.passwordReader = passwordReader;
        this.terminal = terminal;
    }

    public static void main(String[] args) {
        System.exit(run(System.out, System.out.charset(), new PrintWriter(System.err, true), args));
    }

    /**
     * Runs a command line; returns the exit code. Text goes to {@code stdout} in {@code textCharset} (the
     * console's encoding, so it displays correctly); machine-readable output such as JSON is always UTF-8.
     */
    public static int run(OutputStream stdout, Charset textCharset, PrintWriter err, String... args) {
        return run(Main::readPassword, Terminal.SYSTEM, stdout, textCharset, err, args);
    }

    static int run(
            PasswordReader passwordReader, OutputStream stdout, Charset textCharset, PrintWriter err, String... args) {
        return run(passwordReader, Terminal.SYSTEM, stdout, textCharset, err, args);
    }

    static int run(
            PasswordReader passwordReader,
            Terminal terminal,
            OutputStream stdout,
            Charset textCharset,
            PrintWriter err,
            String... args) {
        PrintWriter out = new PrintWriter(new OutputStreamWriter(stdout, textCharset), true);
        Main main = new Main(stdout, passwordReader, terminal);
        CommandLine cli = new CommandLine(main)
                .setOut(out)
                .setErr(err)
                .setCaseInsensitiveEnumValuesAllowed(true)
                .setExecutionStrategy(parseResult -> {
                    // Jackcess warns through JUL in the default locale; its findings are issues in our output
                    JACKCESS_LOG.setLevel(main.verbose ? Level.WARNING : Level.OFF);
                    return new CommandLine.RunLast().execute(parseResult);
                })
                .setExecutionExceptionHandler((e, commandLine, parseResult) -> {
                    commandLine.getErr().println("error: " + message(e));
                    if (main.verbose) {
                        e.printStackTrace(commandLine.getErr());
                    }
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

    /**
     * Progress on standard error: as {@code requested} ({@code --progress} or {@code --no-progress}), or when neither
     * was given, on when the user is at a terminal.
     */
    Progress progress(Boolean requested) {
        boolean on = requested != null ? requested : terminal.interactive();
        return on ? Progress.on(spec.commandLine().getErr(), terminal::nanoTime) : Progress.off();
    }

    /** Asks for the password of a {@code --password} without a value. */
    char[] askPassword() throws IOException {
        return passwordReader.read(spec.commandLine().getErr());
    }

    /**
     * On a console, prompts there and reads without echo. Without one, prompts on stderr and reads the first line of
     * standard input. Never on stdout, which may be JSON going to a file.
     */
    private static char[] readPassword(PrintWriter err) throws IOException {
        Console console = System.console();
        if (console != null) {
            return console.readPassword("Password: ");
        }
        err.print("Password: ");
        err.flush();
        Charset charset = Charset.forName(System.getProperty("native.encoding"), Charset.defaultCharset());
        String line = new BufferedReader(new InputStreamReader(System.in, charset)).readLine();
        return line == null ? null : line.toCharArray();
    }

    /** Whether the user is at a terminal, and the clock progress is timed by: the system's, or a test's. */
    interface Terminal {
        Terminal SYSTEM = new Terminal() {
            @Override
            public boolean interactive() {
                return Main.interactive();
            }

            @Override
            public long nanoTime() {
                return System.nanoTime();
            }
        };

        boolean interactive();

        long nanoTime();
    }

    /**
     * Whether standard input and output are both a terminal. That is all the JDK can tell (02): {@link System#console()}
     * is null unless both are, on Java 21 and 25 alike, and nothing says whether standard error is one. Java 22 to 24
     * return a console even when output is redirected, and only {@code Console.isTerminal()} (Java 22 and later) says
     * so; it is looked up by reflection, since the code is compiled for Java 21.
     */
    static boolean interactive() {
        Console console = System.console();
        if (console == null) {
            return false;
        }
        try {
            return (Boolean) Console.class.getMethod("isTerminal").invoke(console);
        } catch (NoSuchMethodException e) {
            return true; // Java 21: a console exists only on a terminal
        } catch (ReflectiveOperationException | RuntimeException e) {
            return false;
        }
    }

    /** Where an asked-for password comes from: the user, or a fixed answer in tests. */
    @FunctionalInterface
    interface PasswordReader {
        char[] read(PrintWriter err) throws IOException;
    }

    static void requireFile(Path file) throws NoSuchFileException {
        if (!Files.isRegularFile(file)) {
            throw new NoSuchFileException(file.toString(), null, "no such file");
        }
    }

    /**
     * One line for the user. A {@link SourceException} (a database that can't be read) already says why; other I/O
     * errors name their type; anything else is a bug.
     */
    static String message(Throwable e) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t instanceof SourceException source) {
                return source.getMessage();
            }
        }
        String text = e.getClass().getSimpleName() + (e.getMessage() == null ? "" : ": " + e.getMessage());
        return e instanceof IOException ? text : text + " (unexpected; run with --verbose for details)";
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
