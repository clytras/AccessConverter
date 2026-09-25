package io.lytrax.accessconverter.it;

import static org.assertj.core.api.Assertions.assertThat;

import io.lytrax.accessconverter.profile.KeyText;
import io.lytrax.accessconverter.profile.KeyText.Weight;
import io.lytrax.accessconverter.target.mysql.MySqlDialect;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Keeps {@code key-text-weights.tsv} honest (05, Collation): every server of the matrix must still weigh each
 * recorded character exactly as recorded under its dialect's default collation. A server version that changes an
 * equality fails here, loudly, instead of the safe set going stale. With {@code -Dcollation.update=true} the
 * dialect's column is rewritten from the server; then run {@code KeyTextSafetyTest} with the same flag and review
 * both diffs.
 */
class CollationWeightsIT {

    static Stream<String> images() {
        return DatabaseServer.images().stream();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("images")
    void theServerWeighsEveryCharacterAsRecorded(String image) throws Exception {
        DatabaseServer server = DatabaseServer.of(image);
        MySqlDialect dialect = server.dialect();
        Map<Integer, String> measured = new LinkedHashMap<>();
        try (Connection db = server.connect("");
                PreparedStatement query = db.prepareStatement("SELECT HEX(WEIGHT_STRING(CONVERT(CHAR(? USING utf32)"
                        + " USING utf8mb4) COLLATE " + dialect.defaultCollation() + "))")) {
            for (Weight w : KeyText.weights()) {
                query.setInt(1, w.codePoint());
                try (ResultSet rows = query.executeQuery()) {
                    rows.next();
                    measured.put(w.codePoint(), rows.getString(1));
                }
            }
        }
        if (Boolean.getBoolean("collation.update")) {
            rewrite(dialect, measured);
            return;
        }
        List<String> changed = new ArrayList<>();
        for (Weight w : KeyText.weights()) {
            String recorded = dialect == MySqlDialect.MYSQL ? w.mysql() : w.mariadb();
            if (!recorded.equals(measured.get(w.codePoint()))) {
                changed.add(String.format("U+%04X %s -> %s", w.codePoint(), recorded, measured.get(w.codePoint())));
            }
        }
        assertThat(changed)
                .as(
                        "%s weighs these characters differently from key-text-weights.tsv; rerun with"
                                + " -Dcollation.update=true, then KeyTextSafetyTest with the same flag",
                        image)
                .isEmpty();
    }

    private static void rewrite(MySqlDialect dialect, Map<Integer, String> measured) throws Exception {
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
        List<String> out = new ArrayList<>();
        for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
            if (line.startsWith("#") || line.isBlank()) {
                out.add(line);
                continue;
            }
            String[] f = line.split("\t", -1);
            f[dialect == MySqlDialect.MYSQL ? 1 : 2] = measured.get(Integer.parseInt(f[0], 16));
            out.add(String.join("\t", f));
        }
        Files.writeString(file, String.join("\n", out) + "\n", StandardCharsets.UTF_8);
    }
}
