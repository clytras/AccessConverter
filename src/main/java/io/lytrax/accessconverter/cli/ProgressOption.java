package io.lytrax.accessconverter.cli;

import picocli.CommandLine.Option;

/** {@code --progress} / {@code --no-progress}, shared by the commands that read every row: convert and verify. */
final class ProgressOption {

    @Option(
            names = "--progress",
            negatable = true,
            description = "Show progress on standard error: the stage, the table and its rows, on one line that is"
                    + " erased at the end. By default it is on when standard input and output are a terminal;"
                    + " --no-progress turns it off, for instance when standard error goes to a log.")
    Boolean progress;
}
