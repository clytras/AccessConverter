package com.lytrax.accessconverter.regression;

import static org.assertj.core.api.Assertions.assertThat;

import com.lytrax.accessconverter.extract.ExtractOptions;
import com.lytrax.accessconverter.extract.SchemaExtractor;
import com.lytrax.accessconverter.fixtures.Access97Fixture;
import com.lytrax.accessconverter.fixtures.GuestDump;
import com.lytrax.accessconverter.model.SchemaModel;
import com.lytrax.accessconverter.model.TableModel;
import com.lytrax.accessconverter.report.Issue;
import com.lytrax.accessconverter.report.IssueCode;
import com.lytrax.accessconverter.report.Issues;
import com.lytrax.accessconverter.source.AccessSource;
import com.lytrax.accessconverter.source.RowStream;
import com.lytrax.accessconverter.target.json.JsonFixture;
import com.lytrax.accessconverter.target.mysql.MySqlDialect;
import com.lytrax.accessconverter.target.mysql.MySqlFixture;
import com.lytrax.accessconverter.target.sqlite.Sqlite;
import com.lytrax.accessconverter.target.sqlite.SqliteFixture;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A Random autonumber (New Values: Random), made in Access 97's table designer: Access stores it as the default
 * {@code GenUniqueID()} on an ordinary Long autonumber. The model recognizes it instead of reporting an untranslatable
 * default; JSON says {@code "autoNumber": "random"}; SQLite and MySQL, which can only count upward, say so. Above all,
 * the values Jet generated, negative ones and one at INT's ceiling included, stay exactly what Access printed.
 */
class RandomAutoNumberTest {

    private static final Access97Fixture FIXTURE = Access97Fixture.RANDOM_AUTO_NUMBER;

    @TempDir
    Path dir;

    @Test
    void theModelKnowsARandomAutonumberFromAnIncrementOne() throws IOException {
        Issues issues = new Issues();
        SchemaModel model;
        try (AccessSource db = AccessSource.open(FIXTURE.file())) {
            model = SchemaExtractor.extract(db, ExtractOptions.ALL, issues);
        }

        assertThat(model.table("TRandom")
                        .orElseThrow()
                        .column("ID")
                        .orElseThrow()
                        .isRandomAutoNumber())
                .isTrue();
        assertThat(model.table("TIncrement")
                        .orElseThrow()
                        .column("ID")
                        .orElseThrow()
                        .isRandomAutoNumber())
                .isFalse();
        assertThat(issues.list()).noneMatch(i -> i.code() == IssueCode.DEFAULT_UNTRANSLATABLE);
    }

    @Test
    void theValuesAreWhatAccessPrinted() throws IOException {
        GuestDump dump = FIXTURE.dump();
        assertThat(dump.table("TRandom").column("ID"))
                .containsExactly("-2087453651", "-2899478", "1790235740", "2147483647");

        try (AccessSource db = AccessSource.open(FIXTURE.file())) {
            SchemaModel model = SchemaExtractor.extract(db, ExtractOptions.ALL, new Issues());
            for (String name : List.of("TRandom", "TIncrement")) {
                TableModel table = model.table(name).orElseThrow();
                List<String> ids = new ArrayList<>();
                RowStream rows = db.rows(table);
                while (rows.hasNext()) {
                    ids.add(String.valueOf(rows.next()[0]));
                }
                assertThat(ids).as(name).isEqualTo(dump.table(name).column("ID"));
            }
        }
    }

    @Test
    void jsonSaysRandomAndKeepsEveryValue() throws IOException {
        var converted = JsonFixture.convert(FIXTURE.file(), dir.resolve("random.json"));
        String json = Files.readString(converted.output(), StandardCharsets.UTF_8);

        assertThat(json)
                .contains("\"autoNumber\": \"random\"", "\"autoNumber\": \"increment\"")
                .doesNotContain("GenUniqueID")
                .contains(
                        "{\"ID\":-2087453651,\"Name\":\"row 2\"}",
                        "{\"ID\":-2899478,\"Name\":\"row 3\"}",
                        "{\"ID\":1790235740,\"Name\":\"row 1\"}",
                        "{\"ID\":2147483647,\"Name\":\"ceiling\"}");
        assertThat(converted.issues().list()).noneMatch(i -> i.code() == IssueCode.AUTONUMBER_RANDOM_SEQUENTIAL);
        assertThat(converted.verify().matches()).isTrue();
    }

    @Test
    void sqliteSaysItGeneratesInSequenceAndKeepsEveryValue() {
        var converted = SqliteFixture.convert(FIXTURE.file(), dir.resolve("random.sqlite3"));

        assertThat(converted.issues(IssueCode.AUTONUMBER_RANDOM_SEQUENTIAL))
                .singleElement()
                .satisfies(i -> {
                    assertThat(i.table()).isEqualTo("TRandom");
                    assertThat(i.message()).contains("AUTOINCREMENT generates them in sequence", "kept exactly");
                });
        try (Sqlite sqlite = converted.open()) {
            assertThat(sqlite.strings("SELECT ID FROM TRandom ORDER BY ID"))
                    .containsExactly("-2087453651", "-2899478", "1790235740", "2147483647");
        }
        assertThat(converted.verified().differences()).isEmpty();
    }

    @Test
    void mysqlSaysTheNextGeneratedValueOverflows() throws IOException {
        var converted = MySqlFixture.convert(FIXTURE.file(), dir.resolve("random.sql"), MySqlDialect.MYSQL);

        List<Issue> reported = converted.issues(IssueCode.AUTONUMBER_RANDOM_SEQUENTIAL);
        assertThat(reported)
                .singleElement()
                .satisfies(i -> assertThat(i.message())
                        .contains(
                                "AUTO_INCREMENT generates them in sequence",
                                "the largest value is INT's maximum, 2147483647",
                                "the first insert without an ID fails"));
        String dump = Files.readString(converted.file(), StandardCharsets.UTF_8);
        assertThat(dump).contains("(-2087453651, 'row 2')", "(2147483647, 'ceiling')");
    }
}
