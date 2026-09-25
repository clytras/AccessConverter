package io.lytrax.accessconverter.cli;

import io.lytrax.accessconverter.report.Issues;
import io.lytrax.accessconverter.report.Severity;

/** 02, CLI: the process exit codes. */
public final class ExitCodes {
    public static final int OK = 0;
    public static final int WARNINGS = 1;
    public static final int FAILED = 2;
    public static final int USAGE = 64;

    private ExitCodes() {}

    static int of(Issues issues) {
        Severity highest = issues.highestSeverity();
        if (highest == Severity.ERROR) {
            return FAILED;
        }
        return highest == Severity.WARNING ? WARNINGS : OK;
    }
}
