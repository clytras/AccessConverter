package com.lytrax.accessconverter.target;

import com.lytrax.accessconverter.report.IssueCode;
import com.lytrax.accessconverter.report.Issues;
import com.lytrax.accessconverter.value.CanonicalText;
import java.util.Map;
import java.util.TreeMap;

/**
 * Hands out the identifiers a target may use, keeping them unique and legal. Access names are kept as they are
 * wherever the target allows it; anything renamed raises {@link IssueCode#IDENTIFIER_RENAMED}.
 *
 * <p>One policy covers one namespace. SQLite puts tables and indexes in the same namespace, so both go through the
 * same instance.
 */
public final class IdentifierPolicy {
    /** SQLite keeps names beginning with this prefix for itself. */
    private static final String RESERVED_PREFIX = "sqlite_";

    /** Characters of the hash appended to a name that would collide. */
    private static final int HASH_LENGTH = 6;

    private final Map<String, String> taken = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

    /**
     * Registers a name and returns the one to use: the wanted name when it is legal and free, otherwise a renamed
     * one derived from {@code context}, which must identify the object uniquely so the result is deterministic.
     *
     * @param what what is being named, for the report ("table", "index")
     */
    public String register(String wanted, String context, String what, Issues issues, String table) {
        String candidate = legal(wanted);
        String reason = candidate.equals(wanted) ? null : "the name is reserved for SQLite's own objects";
        if (taken.containsKey(candidate)) {
            String hashed = candidate + "_" + hash(context);
            for (int n = 2; taken.containsKey(hashed); n++) {
                hashed = candidate + "_" + hash(context) + "_" + n;
            }
            candidate = hashed;
            reason = "the name is already used in this database";
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

    /** Whether this name is already handed out (case-insensitively, as SQLite compares ASCII names). */
    public boolean isTaken(String name) {
        return taken.containsKey(name);
    }

    private static String legal(String wanted) {
        return wanted.regionMatches(true, 0, RESERVED_PREFIX, 0, RESERVED_PREFIX.length()) ? "_" + wanted : wanted;
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
