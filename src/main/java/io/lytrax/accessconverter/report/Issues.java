package io.lytrax.accessconverter.report;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Collects issues from every pipeline stage. Occurrences with the same code, table and object are aggregated: the
 * first message is kept, the count grows and up to {@value #MAX_SAMPLES} samples are retained.
 */
public final class Issues {
    public static final int MAX_SAMPLES = 5;

    private static final Comparator<String> NULLS_FIRST_CI =
            Comparator.nullsFirst(String.CASE_INSENSITIVE_ORDER.thenComparing(Comparator.naturalOrder()));
    private static final Comparator<Issue> ORDER = Comparator.comparing(Issue::table, NULLS_FIRST_CI)
            .thenComparing(Issue::code)
            .thenComparing(Issue::object, NULLS_FIRST_CI);

    private final Map<Key, Entry> entries = new LinkedHashMap<>();

    public void add(IssueCode code, String table, String object, String message) {
        add(code, table, object, message, null);
    }

    public void add(IssueCode code, String table, String object, String message, String sample) {
        Entry entry = entries.computeIfAbsent(new Key(code, table, object), k -> new Entry(message));
        entry.count++;
        if (sample != null && entry.samples.size() < MAX_SAMPLES) {
            entry.samples.add(sample);
        }
    }

    public boolean isEmpty() {
        return entries.isEmpty();
    }

    public boolean has(IssueCode code) {
        return entries.keySet().stream().anyMatch(k -> k.code == code);
    }

    /** Aggregated issues in a deterministic order: by table (global issues first), code, then object. */
    public List<Issue> list() {
        List<Issue> issues = new ArrayList<>(entries.size());
        entries.forEach((k, e) -> issues.add(new Issue(k.code, k.table, k.object, e.message, e.count, e.samples)));
        issues.sort(ORDER);
        return issues;
    }

    public Severity highestSeverity() {
        return entries.keySet().stream()
                .map(k -> k.code.severity())
                .max(Comparator.naturalOrder())
                .orElse(null);
    }

    private record Key(IssueCode code, String table, String object) {
        Key {
            Objects.requireNonNull(code, "code");
        }
    }

    private static final class Entry {
        final String message;
        final List<String> samples = new ArrayList<>();
        long count;

        Entry(String message) {
            this.message = Objects.requireNonNull(message, "message");
        }
    }
}
