package com.lytrax.accessconverter.report;

import java.util.List;
import java.util.Objects;

/**
 * One report entry: a code, where it applies ({@code table} and {@code object} may be null) and how often. Repeated
 * occurrences of the same code on the same object are aggregated into {@code count}, with a few {@code samples}.
 */
public record Issue(IssueCode code, String table, String object, String message, long count, List<String> samples) {

    public Issue {
        Objects.requireNonNull(code, "code");
        Objects.requireNonNull(message, "message");
        samples = List.copyOf(samples);
    }

    public Severity severity() {
        return code.severity();
    }
}
