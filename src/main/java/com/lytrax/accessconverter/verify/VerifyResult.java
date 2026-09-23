package com.lytrax.accessconverter.verify;

import com.lytrax.accessconverter.report.IssueCode;
import com.lytrax.accessconverter.report.Issues;
import java.util.ArrayList;
import java.util.List;

/**
 * What {@code verify} found when it compared an output with the plan and the source (09): the differences, of which
 * the first {@value #MAX_DIFFERENCES} are kept, and the rows each table held on both sides.
 */
public record VerifyResult(List<Difference> differences, long differenceCount, List<TableRows> tables) {

    /** How many differences are kept for the report. */
    public static final int MAX_DIFFERENCES = 20;

    public VerifyResult {
        differences = List.copyOf(differences);
        tables = List.copyOf(tables);
    }

    public boolean matches() {
        return differenceCount == 0;
    }

    /** Every difference is a converter bug, so each one is an error in the report. */
    public void report(Issues issues) {
        for (Difference difference : differences) {
            issues.add(
                    IssueCode.VERIFY_DIFFERENCE,
                    difference.table(),
                    difference.object(),
                    "verify: " + difference.what() + " is " + difference.actual() + ", expected "
                            + difference.expected());
        }
        if (differenceCount > differences.size()) {
            issues.add(
                    IssueCode.VERIFY_DIFFERENCE,
                    null,
                    null,
                    "verify found " + differenceCount + " differences in all; the first " + differences.size()
                            + " are listed");
        }
    }

    /** @param what the kind of difference, for the message */
    public record Difference(String table, String object, String what, String expected, String actual) {

        @Override
        public String toString() {
            return table + (object == null ? "" : "." + object) + ": " + what + " is " + actual + ", expected "
                    + expected;
        }
    }

    public record TableRows(String table, long expected, long actual) {}

    /** Collects differences while a verifier runs: all are counted, the first few kept. */
    public static final class Collector {
        private final List<Difference> kept = new ArrayList<>();
        private final List<TableRows> tables = new ArrayList<>();
        private long count;

        public void add(Difference difference) {
            count++;
            if (kept.size() < MAX_DIFFERENCES) {
                kept.add(difference);
            }
        }

        public void add(String table, String object, String what, String expected, String actual) {
            add(new Difference(table, object, what, expected, actual));
        }

        /** Takes over another collector's differences, as if they had been added here. */
        public void addAll(Collector other) {
            for (Difference difference : other.kept) {
                if (kept.size() < MAX_DIFFERENCES) {
                    kept.add(difference);
                }
            }
            count += other.count;
            tables.addAll(other.tables);
        }

        public void rows(TableRows rows) {
            tables.add(rows);
        }

        public long count() {
            return count;
        }

        public VerifyResult result() {
            return new VerifyResult(kept, count, tables);
        }
    }
}
