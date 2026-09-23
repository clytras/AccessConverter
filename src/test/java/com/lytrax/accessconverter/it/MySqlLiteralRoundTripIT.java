package com.lytrax.accessconverter.it;

import static org.assertj.core.api.Assertions.assertThat;

import com.lytrax.accessconverter.target.mysql.MySqlDumpWriter;
import com.lytrax.accessconverter.target.mysql.MySqlLiterals;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * F-02 through a real server (09, Property layer): random text, heavy in what escaping gets wrong (NUL, quotes,
 * backslashes, Ctrl-Z, line breaks, LIKE wildcards, emoji, right-to-left text), is written as literals, imported with
 * the server's client and read back unchanged.
 */
class MySqlLiteralRoundTripIT {

    private static final String AWKWARD = "\0'\"\\\b\n\r\t\u001A%_ ;-/*#`";

    private static final String[] WORDS = {
        "C:\\temp\\new",
        "Lors des \\/D1",
        "Γιώργος",
        "שלום",
        "مرحبا",
        "😀",
        "\uD83D\uDC68\u200D\uD83D\uDC69",
        "a\u00A0b",
        "--",
        "/*!",
        "*/"
    };

    @TempDir
    static Path dir;

    static Stream<String> images() {
        return DatabaseServer.images().stream();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("images")
    void randomTextReadsBackUnchanged(String image) throws Exception {
        List<String> values = values(2000);
        StringBuilder dump = new StringBuilder("SET NAMES utf8mb4;\nSET SESSION sql_mode = '")
                .append(MySqlDumpWriter.SQL_MODE)
                .append("';\nCREATE TABLE r (id INT NOT NULL PRIMARY KEY, v LONGTEXT) DEFAULT CHARSET=utf8mb4;\n");
        for (int i = 0; i < values.size(); i++) {
            dump.append(i % 100 == 0 ? (i == 0 ? "" : ";\n") + "INSERT INTO r VALUES\n" : ",\n")
                    .append('(')
                    .append(i)
                    .append(", ")
                    .append(MySqlLiterals.string(values.get(i)))
                    .append(')');
        }
        dump.append(";\n");
        Path file =
                Files.writeString(dir.resolve(image.replace(':', '-') + "-literals.sql"), dump, StandardCharsets.UTF_8);

        DatabaseServer server = DatabaseServer.of(image);
        server.recreate("literals");
        assertThat(server.importDump(file, "literals")).isEmpty();
        try (Connection db = server.connect("literals");
                Statement s = db.createStatement();
                ResultSet rows = s.executeQuery("SELECT id, v FROM r ORDER BY id")) {
            int n = 0;
            while (rows.next()) {
                assertThat(rows.getString(2)).as("value %d", rows.getInt(1)).isEqualTo(values.get(rows.getInt(1)));
                n++;
            }
            assertThat(n).isEqualTo(values.size());
        }
    }

    private static List<String> values(int count) {
        Random random = new Random(20260923);
        List<String> values = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            StringBuilder text = new StringBuilder();
            int parts = random.nextInt(8);
            for (int p = 0; p < parts; p++) {
                switch (random.nextInt(4)) {
                    case 0 -> text.append(AWKWARD.charAt(random.nextInt(AWKWARD.length())));
                    case 1 -> text.append(WORDS[random.nextInt(WORDS.length)]);
                    case 2 -> text.appendCodePoint(random.nextInt(0x20, 0x7F));
                    default -> {
                        int codePoint;
                        do {
                            codePoint = random.nextInt(0x10FFFF);
                        } while (Character.getType(codePoint) == Character.SURROGATE
                                || Character.getType(codePoint) == Character.UNASSIGNED);
                        text.appendCodePoint(codePoint);
                    }
                }
            }
            values.add(text.toString());
        }
        return values;
    }
}
