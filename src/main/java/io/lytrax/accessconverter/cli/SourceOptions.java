package io.lytrax.accessconverter.cli;

import io.lytrax.accessconverter.source.OpenOptions;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;
import java.nio.charset.UnsupportedCharsetException;
import java.nio.file.Files;
import java.nio.file.Path;
import picocli.CommandLine;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;

/** How to open the source database: shared by every command that reads one. */
final class SourceOptions {
    @Spec(Spec.Target.MIXEE)
    CommandSpec spec;

    /** What picocli assigns to a --password without a value: ask for it. Nobody can type it as a password. */
    private static final String ASK = "\u0000ask";

    @Option(
            names = "--password",
            arity = "0..1",
            fallbackValue = ASK,
            paramLabel = "<password>",
            defaultValue = "${env:ACCESSCONVERTER_PASSWORD}",
            description = {
                "Password of an encrypted database. Alone, it is asked for without echo (put it after the input,"
                        + " or the input is taken as the password). The ACCESSCONVERTER_PASSWORD environment variable"
                        + " keeps it out of the process list."
            })
    char[] password;

    @Option(
            names = "--charset",
            paramLabel = "<name>",
            description = {
                "Access 97 only: decode text with this charset (e.g. windows-1253) instead of the code page in the"
                        + " file's header. Access 2000 and later store Unicode and ignore it."
            })
    String charset;

    /** What happens to linked tables (D11). */
    enum Linked {
        skip,
        resolve
    }

    @Option(
            names = "--linked",
            paramLabel = "<mode>",
            description = {
                "Linked tables: skip (the default) lists and reports them without their data; resolve reads a table"
                        + " linked to another Access file from that file, found by its file name in --linked-root."
                        + " ODBC links are always skipped."
            })
    Linked linked;

    @Option(
            names = "--linked-root",
            paramLabel = "<dir>",
            description = {
                "With --linked resolve: the only directory the back-end files are looked for in (default: the"
                        + " input's directory). The paths Access stored are never opened."
            })
    Path linkedRoot;

    boolean resolvesLinks() {
        return linked == Linked.resolve;
    }

    OpenOptions toOpenOptions() throws IOException {
        if (linkedRoot != null && !resolvesLinks()) {
            throw new CommandLine.ParameterException(spec.commandLine(), "--linked-root applies to --linked resolve");
        }
        if (linkedRoot != null && !Files.isDirectory(linkedRoot)) {
            throw new CommandLine.ParameterException(
                    spec.commandLine(), "--linked-root: " + linkedRoot + " is not a directory");
        }
        Charset decoded = null;
        if (charset != null) {
            try {
                decoded = Charset.forName(charset);
            } catch (IllegalCharsetNameException | UnsupportedCharsetException e) {
                throw new CommandLine.ParameterException(spec.commandLine(), "unknown charset: " + charset);
            }
        }
        char[] given = password;
        if (given != null && ASK.equals(new String(given))) {
            given = ((Main) spec.root().userObject()).askPassword();
        }
        String pw = given == null || given.length == 0 ? null : new String(given);
        return new OpenOptions(
                pw, decoded, resolvesLinks() ? OpenOptions.Links.resolve(linkedRoot) : OpenOptions.Links.SKIP);
    }
}
