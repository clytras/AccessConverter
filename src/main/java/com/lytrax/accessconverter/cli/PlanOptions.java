package com.lytrax.accessconverter.cli;

import com.lytrax.accessconverter.extract.ExtractOptions;
import com.lytrax.accessconverter.target.ConvertOptions;
import com.lytrax.accessconverter.target.ConvertOptions.OnTableError;
import com.lytrax.accessconverter.target.sqlite.SqliteOptions;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.regex.Pattern;
import picocli.CommandLine.Option;

/**
 * The options that decide what the output contains, shared by {@code convert} and {@code verify}: {@code verify}
 * re-plans the conversion from the source, so it has to be told the same things (09, step 1).
 */
final class PlanOptions {

    @Option(
            names = "--tables",
            paramLabel = "<glob,…>",
            split = ",",
            description = "Only these tables (glob patterns, * and ? allowed).")
    List<String> tables;

    @Option(
            names = "--exclude-tables",
            paramLabel = "<glob,…>",
            split = ",",
            description = "Leave these tables out (glob patterns).")
    List<String> excludeTables;

    @Option(
            names = "--no-profile",
            description = "Skip the profiling pass. Faster, but every constraint that depends on the data is then"
                    + " left out and exact decimals are stored as text.")
    boolean noProfile;

    @Option(names = "--include-hidden", description = "Also write Access's own hidden columns (s_GUID, s_Lineage, …).")
    boolean includeHidden;

    @Option(names = "--sqlite-strict", description = "SQLite: emit STRICT tables (needs SQLite 3.37 to read).")
    boolean sqliteStrict;

    @Option(
            names = "--sqlite-nocase",
            description = "SQLite: COLLATE NOCASE on every text column, so comparisons ignore case as Access does.")
    boolean sqliteNocase;

    @Option(
            names = "--sqlite-metadata",
            description = "SQLite: add the _access_columns and _access_relationships tables with the full Access"
                    + " metadata.")
    boolean sqliteMetadata;

    ConvertOptions convertOptions(OnTableError onTableError, int batchRows) {
        return new ConvertOptions(!noProfile, includeHidden, onTableError, batchRows);
    }

    SqliteOptions sqliteOptions(boolean analyze) {
        return new SqliteOptions(sqliteStrict, sqliteNocase, sqliteMetadata, analyze);
    }

    ExtractOptions extractOptions() {
        if (tables == null && excludeTables == null) {
            return ExtractOptions.ALL;
        }
        List<Pattern> include = globs(tables);
        List<Pattern> exclude = globs(excludeTables);
        return new ExtractOptions(name -> (include.isEmpty()
                        || include.stream().anyMatch(p -> p.matcher(name).matches()))
                && exclude.stream().noneMatch(p -> p.matcher(name).matches()));
    }

    /** Adds what these options were to a report's option map. */
    SortedMap<String, String> describe() {
        SortedMap<String, String> described = new TreeMap<>();
        described.put("profile", String.valueOf(!noProfile));
        described.put("includeHidden", String.valueOf(includeHidden));
        described.put("sqliteStrict", String.valueOf(sqliteStrict));
        described.put("sqliteNocase", String.valueOf(sqliteNocase));
        described.put("sqliteMetadata", String.valueOf(sqliteMetadata));
        if (tables != null) {
            described.put("tables", String.join(",", tables));
        }
        if (excludeTables != null) {
            described.put("excludeTables", String.join(",", excludeTables));
        }
        return described;
    }

    /** Access names compare case-insensitively, and so do the patterns that select them. */
    private static List<Pattern> globs(List<String> globs) {
        if (globs == null) {
            return List.of();
        }
        List<Pattern> patterns = new ArrayList<>();
        for (String glob : globs) {
            StringBuilder regex = new StringBuilder();
            for (char c : glob.strip().toCharArray()) {
                switch (c) {
                    case '*' -> regex.append(".*");
                    case '?' -> regex.append('.');
                    default -> regex.append(Pattern.quote(String.valueOf(c)));
                }
            }
            patterns.add(Pattern.compile(regex.toString(), Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE));
        }
        return patterns;
    }

    static String lower(Enum<?> value) {
        return value.name().toLowerCase(Locale.ROOT);
    }
}
