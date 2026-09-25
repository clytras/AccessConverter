package io.lytrax.accessconverter.target;

import io.lytrax.accessconverter.report.IssueCode;
import io.lytrax.accessconverter.report.Issues;
import io.lytrax.accessconverter.value.CanonicalText;
import java.util.Map;
import java.util.TreeMap;

/**
 * Hands out the identifiers a target may use, keeping them unique and legal. Access names are kept as they are
 * wherever the target allows it; anything renamed raises {@link IssueCode#IDENTIFIER_RENAMED}.
 *
 * <p>One policy covers one namespace: SQLite puts tables and indexes in the same one, MySQL has one per database for
 * tables and constraints and one per table for columns and indexes. What a target accepts is its {@link Rules}.
 */
public final class IdentifierPolicy {

    /** Characters of the hash appended to a name that would collide or had to be shortened. */
    private static final int HASH_LENGTH = 6;

    /** What a target accepts as a name. */
    public interface Rules {
        /** The longest name, in characters (code points), or 0 for no limit. */
        int maxLength();

        /** The name made legal apart from its length, or the name itself when it already is. */
        String legal(String wanted);

        /** Why {@link #legal} had to change a name, for the report. */
        String illegalReason();
    }

    /** SQLite keeps names beginning with {@code sqlite_} for itself, and has no length limit. */
    public static final Rules SQLITE = new Rules() {
        private static final String RESERVED_PREFIX = "sqlite_";

        @Override
        public int maxLength() {
            return 0;
        }

        @Override
        public String legal(String wanted) {
            return wanted.regionMatches(true, 0, RESERVED_PREFIX, 0, RESERVED_PREFIX.length()) ? "_" + wanted : wanted;
        }

        @Override
        public String illegalReason() {
            return "the name is reserved for SQLite's own objects";
        }
    };

    /**
     * MySQL and MariaDB: at most 64 characters, no trailing spaces, and only characters of the Basic Multilingual
     * Plane (a quoted identifier may hold anything else except NUL).
     */
    public static final Rules MYSQL = new Rules() {
        @Override
        public int maxLength() {
            return 64;
        }

        @Override
        public String legal(String wanted) {
            StringBuilder legal = new StringBuilder(wanted.length());
            wanted.codePoints().forEach(c -> {
                if (c == 0 || c > 0xFFFF) {
                    legal.append('_');
                } else {
                    legal.appendCodePoint(c);
                }
            });
            String stripped = legal.toString().stripTrailing();
            return stripped.isEmpty() ? "_" : stripped;
        }

        @Override
        public String illegalReason() {
            return "MySQL doesn't allow the name's trailing spaces or characters outside the Basic Multilingual Plane";
        }
    };

    private final Rules rules;
    private final Map<String, String> taken = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

    public IdentifierPolicy(Rules rules) {
        this.rules = rules;
    }

    /**
     * Registers a name and returns the one to use: the wanted name when it is legal and free, otherwise a renamed
     * one derived from {@code context}, which must identify the object uniquely so the result is deterministic.
     *
     * @param what what is being named, for the report ("table", "index")
     */
    public String register(String wanted, String context, String what, Issues issues, String table) {
        String candidate = rules.legal(wanted);
        String reason = candidate.equals(wanted) ? null : rules.illegalReason();
        if (tooLong(candidate)) {
            candidate = shorten(candidate, hash(context));
            reason = "the name is longer than " + rules.maxLength() + " characters";
        }
        if (taken.containsKey(candidate)) {
            String hashed = suffixed(candidate, hash(context));
            for (int n = 2; taken.containsKey(hashed); n++) {
                hashed = suffixed(candidate, hash(context) + "_" + n);
            }
            candidate = hashed;
            reason = "the name is already used in this " + (rules == SQLITE ? "database" : "namespace");
        }
        if (reason != null) {
            issues.add(
                    IssueCode.IDENTIFIER_RENAMED,
                    table,
                    wanted,
                    what + " " + wanted + " is written as " + candidate + ": " + reason);
        }
        taken.put(candidate, candidate);
        return candidate;
    }

    /** Whether this name is already handed out (case-insensitively, as SQLite and MySQL compare names). */
    public boolean isTaken(String name) {
        return taken.containsKey(name);
    }

    private boolean tooLong(String name) {
        return rules.maxLength() > 0 && name.codePointCount(0, name.length()) > rules.maxLength();
    }

    /** {@code name_suffix}, with the name cut short so the whole stays within the length limit. */
    private String suffixed(String name, String suffix) {
        String joined = name + "_" + suffix;
        return tooLong(joined) ? shorten(name, suffix) : joined;
    }

    private String shorten(String name, String suffix) {
        int keep = rules.maxLength() - suffix.length() - 1;
        String head =
                name.substring(0, name.offsetByCodePoints(0, Math.min(keep, name.codePointCount(0, name.length()))));
        return rules.legal(head) + "_" + suffix;
    }

    private static String hash(String context) {
        return CanonicalText.sha256(context).substring(0, HASH_LENGTH);
    }

    /** An SQL identifier in double quotes, with {@code "} doubled (06, Identifiers). */
    public static String quote(String name) {
        return "\"" + name.replace("\"", "\"\"") + "\"";
    }

    /** A single-quoted SQL string literal, with {@code '} doubled. */
    public static String literal(String text) {
        return "'" + text.replace("'", "''") + "'";
    }
}
