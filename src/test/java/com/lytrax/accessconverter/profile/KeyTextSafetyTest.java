package com.lytrax.accessconverter.profile;

import static org.assertj.core.api.Assertions.assertThat;

import com.healthmarketscience.jackcess.ColumnBuilder;
import com.healthmarketscience.jackcess.ConstraintViolationException;
import com.healthmarketscience.jackcess.DataType;
import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.DatabaseBuilder;
import com.healthmarketscience.jackcess.IndexBuilder;
import com.healthmarketscience.jackcess.Table;
import com.healthmarketscience.jackcess.TableBuilder;
import com.lytrax.accessconverter.profile.KeyText.Weight;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Derives which key characters the default collations compare as Access does, from the weights the servers
 * reported ({@code CollationWeightsIT} keeps those current) and Access's own index encoding, and pins the result in
 * {@code key-text-weights.tsv}. Run with {@code -Dcollation.update=true} to rewrite the {@code safe} column, then
 * review the diff.
 *
 * <p>The rule, per collation: a character with more than one primary weight (an expansion such as {@code Æ} =
 * {@code AE}) or none (an ignorable) is unsafe. Characters with the same weight are one key to the collation; Access
 * splits them into its own equality classes, and only the largest class stays safe.
 * Two safe characters are then equal to the collation only when they are equal to Access, so a key made of them never
 * collides where Access's didn't. A character is safe only when it is safe under both collations.
 */
class KeyTextSafetyTest {

    /** The weights below this are secondary; a primary weight is never this small (UCA). */
    private static final int LOWEST_PRIMARY = 0x0200;

    @Test
    void theSafeSetFollowsFromTheWeightsAndAccess(@TempDir Path dir) throws IOException {
        List<Weight> weights = KeyText.weights();
        Map<Integer, Boolean> derived = new LinkedHashMap<>();
        try (Database access = new DatabaseBuilder(dir.resolve("classes.accdb"))
                .setFileFormat(Database.FileFormat.V2010)
                .create()) {
            Set<Integer> mysql = safe(weights, Weight::mysql, KeyTextSafetyTest::mysqlPrimaries, access, "m");
            Set<Integer> mariadb = safe(weights, Weight::mariadb, KeyTextSafetyTest::mariadbPrimaries, access, "d");
            for (Weight w : weights) {
                derived.put(w.codePoint(), mysql.contains(w.codePoint()) && mariadb.contains(w.codePoint()));
            }
        }
        if (Boolean.getBoolean("collation.update")) {
            rewrite(derived);
            return;
        }
        for (Weight w : weights) {
            assertThat(w.safe())
                    .as(
                            "U+%04X is %s; run with -Dcollation.update=true and review the diff",
                            w.codePoint(), derived.get(w.codePoint()) ? "safe" : "unsafe")
                    .isEqualTo(derived.get(w.codePoint()));
        }
    }

    /** The classes the measurements that motivated this (05) must stay on the unsafe side of. */
    @Test
    void theMeasuredDifferencesAreUnsafe() {
        assertThat(KeyText.isSafe("Code")).isTrue();
        assertThat(KeyText.isSafe("résumé Ελλάδα Москва")).isTrue();
        assertThat(KeyText.isSafe("a\u00A0b")).as("no-break space").isFalse();
        assertThat(KeyText.isSafe("\u0001\u0002\u0003"))
                .as("control characters (money2002.mny)")
                .isFalse();
        assertThat(KeyText.isSafe("a\u200Bb")).as("zero-width space").isFalse();
        assertThat(KeyText.isSafe("\u00AA")).as("feminine ordinal").isFalse();
        assertThat(KeyText.isSafe("\u2126")).as("ohm sign").isFalse();
        assertThat(KeyText.isSafe("\u3041")).as("kana").isFalse();
        assertThat(KeyText.isSafe("\uD83D\uDE00")).as("outside the BMP").isFalse();
    }

    private static Set<Integer> safe(
            List<Weight> weights,
            Function<Weight, String> weightOf,
            java.util.function.ToIntFunction<String> primaries,
            Database access,
            String prefix)
            throws IOException {
        Map<String, List<Integer>> groups = new LinkedHashMap<>();
        for (Weight w : weights) {
            String weight = weightOf.apply(w);
            if (primaries.applyAsInt(weight) == 1) {
                groups.computeIfAbsent(weight, k -> new ArrayList<>()).add(w.codePoint());
            }
        }
        Set<Integer> safe = new TreeSet<>();
        int n = 0;
        for (List<Integer> group : groups.values()) {
            if (group.size() == 1) {
                safe.addAll(group);
                continue;
            }
            safe.addAll(largestAccessClass(group, access, prefix + n++));
        }
        return safe;
    }

    /**
     * Access's largest equality class within a group the collation treats as one key (ties go to the class with the
     * lowest code point): {@code Μ}, {@code μ} and the micro sign are one key to the collations, and Access keeps
     * {@code Μ}/{@code μ} together and the micro sign apart, so the letters stay safe and the sign doesn't.
     */
    private static List<Integer> largestAccessClass(List<Integer> group, Database access, String name)
            throws IOException {
        List<List<Integer>> classes = new ArrayList<>();
        for (int codePoint : group) {
            List<Integer> home = null;
            for (List<Integer> candidate : classes) {
                if (equalInAccess(
                        candidate.get(0), codePoint, access, name + "_" + codePoint + "_" + candidate.get(0))) {
                    home = candidate;
                    break;
                }
            }
            if (home == null) {
                home = new ArrayList<>();
                classes.add(home);
            }
            home.add(codePoint);
        }
        List<Integer> largest = classes.get(0);
        for (List<Integer> candidate : classes) {
            if (candidate.size() > largest.size()) {
                largest = candidate;
            }
        }
        return largest;
    }

    /**
     * Whether Access's unique index takes the two characters for one key: the second insert is rejected. Each stands
     * between two letters, so no trailing-space rule interferes.
     */
    private static boolean equalInAccess(int first, int second, Database access, String name) throws IOException {
        Table table = new TableBuilder(name)
                .addColumn(new ColumnBuilder("v", DataType.TEXT).setLengthInUnits(10))
                .addIndex(new IndexBuilder("u").addColumns("v").setUnique())
                .toTable(access);
        table.addRow(wrap(first));
        try {
            table.addRow(wrap(second));
            return false;
        } catch (ConstraintViolationException sameKey) {
            return true;
        }
    }

    /** MySQL writes the primary weights, a {@code 0000} separator, then the secondary ones. */
    static int mysqlPrimaries(String weight) {
        int n = 0;
        for (int i = 0; i + 4 <= weight.length() && !weight.startsWith("0000", i); i += 4) {
            n++;
        }
        return n;
    }

    /** MariaDB writes the primary weights and then the secondary ones, without a separator. */
    static int mariadbPrimaries(String weight) {
        int n = 0;
        for (int i = 0;
                i + 4 <= weight.length() && Integer.parseInt(weight.substring(i, i + 4), 16) >= LOWEST_PRIMARY;
                i += 4) {
            n++;
        }
        return n;
    }

    private static String wrap(int codePoint) {
        return "x" + Character.toString(codePoint) + "x";
    }

    private static void rewrite(Map<Integer, Boolean> derived) throws IOException {
        Path file = Path.of(
                System.getProperty("basedir", "."),
                "src",
                "main",
                "resources",
                "com",
                "lytrax",
                "accessconverter",
                "profile",
                KeyText.RESOURCE);
        List<String> lines = Files.readAllLines(file, StandardCharsets.UTF_8);
        List<String> out = lines.stream()
                .map(line -> {
                    if (line.startsWith("#") || line.isBlank()) {
                        return line;
                    }
                    String[] f = line.split("\t", -1);
                    return f[0] + "\t" + f[1] + "\t" + f[2] + "\t" + (derived.get(Integer.parseInt(f[0], 16)) ? 1 : 0);
                })
                .collect(Collectors.toList());
        Files.writeString(file, String.join("\n", out) + "\n", StandardCharsets.UTF_8);
    }
}
