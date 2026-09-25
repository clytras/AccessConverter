package io.lytrax.accessconverter.regression;

import static org.assertj.core.api.Assertions.assertThat;

import io.lytrax.accessconverter.fixtures.CorpusFile;
import io.lytrax.accessconverter.fixtures.GeneratedFixture;
import io.lytrax.accessconverter.report.IssueCode;
import io.lytrax.accessconverter.target.ConvertOptions;
import io.lytrax.accessconverter.target.sqlite.Sqlite;
import io.lytrax.accessconverter.target.sqlite.SqliteFixture;
import io.lytrax.accessconverter.target.sqlite.SqliteFixture.Converted;
import io.lytrax.accessconverter.target.sqlite.SqliteOptions;
import java.nio.file.Path;
import java.util.TimeZone;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The value findings of 01 in the SQLite output: unsigned Byte (F-01), NULL numbers that stay NULL (F-04), dates as
 * local ISO text instead of epoch integers (F-05), exact decimals (F-06), Single values that don't gain digits
 * (F-07), Large Number and Date/Time Extended (F-10) and raw OLE bytes (F-11).
 */
class SqliteValueFidelityTest {

    @TempDir
    static Path dir;

    static Converted schemaFidelity;

    @BeforeAll
    static void convert() {
        schemaFidelity =
                SqliteFixture.convert(GeneratedFixture.SCHEMA_FIDELITY.path(), dir.resolve("schemaFidelity.sqlite3"));
    }

    @Test
    void byteValuesOver127StayPositive() {
        try (Sqlite sqlite = schemaFidelity.open()) {
            // Jackcess hands Access's unsigned Byte over as a signed Java byte: 200 was -56 and 255 was -1
            assertThat(sqlite.strings("SELECT Rating FROM Customers ORDER BY CustomerID"))
                    .containsExactly("200", "255", "0");
        }
    }

    @Test
    void aNullNumberStaysNull() {
        try (Sqlite sqlite = schemaFidelity.open()) {
            // v2 wrote 0 for every NULL Single, Double, Currency and Decimal (F-04)
            assertThat(sqlite.value("SELECT Balance FROM Customers WHERE Code = 'resume'"))
                    .isNull();
            assertThat(sqlite.value("SELECT Discount FROM Customers WHERE Code = 'resume'"))
                    .isNull();
            assertThat(sqlite.value("SELECT count(*) FROM Customers WHERE Balance IS NULL"))
                    .isEqualTo(1);
        }
    }

    @Test
    void datesAreLocalIsoTextWhateverTheMachinesTimeZone() {
        TimeZone original = TimeZone.getDefault();
        try {
            TimeZone.setDefault(TimeZone.getTimeZone("Pacific/Kiritimati"));
            Converted far =
                    SqliteFixture.convert(GeneratedFixture.SCHEMA_FIDELITY.path(), dir.resolve("timezone.sqlite3"));
            TimeZone.setDefault(TimeZone.getTimeZone("America/Anchorage"));
            Converted near =
                    SqliteFixture.convert(GeneratedFixture.SCHEMA_FIDELITY.path(), dir.resolve("timezone2.sqlite3"));
            assertThat(Sqlite.dump(far.file())).isEqualTo(Sqlite.dump(near.file()));
            try (Sqlite sqlite = far.open()) {
                assertThat(sqlite.value("SELECT Created FROM Customers WHERE Code = 'C1'"))
                        .isEqualTo("2024-01-02 03:04:05");
                // A time-only Access value is on Access's day zero
                assertThat(sqlite.value("SELECT OrderDate FROM Orders WHERE OrderID = 1"))
                        .isEqualTo("1899-12-30 10:30:00");
                // Access dates reach back to year 100, which needs four digits to stay sortable
                assertThat(sqlite.value("SELECT Created FROM Customers WHERE Code = 'résumé'"))
                        .isEqualTo("0201-05-05 00:00:00");
                // SQLite's own date functions read the format directly
                assertThat(sqlite.value("SELECT strftime('%Y', Created) FROM Customers WHERE Code = 'C1'"))
                        .isEqualTo("2024");
            }
        } finally {
            TimeZone.setDefault(original);
        }
    }

    @Test
    void exactDecimalsSurviveEitherAsNumbersOrAsText() {
        try (Sqlite sqlite = schemaFidelity.open()) {
            // 19 significant digits: NUMERIC affinity would store 123456789012345.13 (F-06)
            assertThat(sqlite.value("SELECT Balance FROM Customers WHERE Code = 'C1'"))
                    .isEqualTo("123456789012345.1234");
            assertThat(sqlite.value("SELECT typeof(Balance) FROM Customers WHERE Code = 'C1'"))
                    .isEqualTo("text");
            // Six digits fit a REAL exactly, so the column stays a number
            assertThat(sqlite.value("SELECT Discount FROM Customers WHERE Code = 'C1'"))
                    .isEqualTo(12.3456);
            assertThat(sqlite.value("SELECT typeof(Discount) FROM Customers WHERE Code = 'C1'"))
                    .isEqualTo("real");
        }
        assertThat(schemaFidelity.issues(IssueCode.DECIMAL_STORED_AS_TEXT))
                .singleElement()
                .satisfies(issue -> assertThat(issue.object()).isEqualTo("Balance"));
    }

    @Test
    void aSingleKeepsItsOwnShortestForm() {
        Converted converted = SqliteFixture.convert(
                CorpusFile.get("jackcess/V2010/calcFieldV2010.accdb").file(), dir.resolve("calcField.sqlite3"));
        try (Sqlite sqlite = converted.open()) {
            // v2's JSON rounded a Single to two decimals: 2583.2092 became 2583.21 and 0.0035889593 became 0.0
            // (F-07). Binding Double.parseDouble(Float.toString(v)) keeps the float's own shortest form.
            assertThat(sqlite.strings("SELECT CAST(FloatTest AS TEXT) FROM Table1 ORDER BY _rowid_"))
                    .containsExactly("2583.2092", "0.0035889593", "0.0", "1.27413e-10");
        }
    }

    @Test
    void largeNumbersAndExtendedDatesAreWrittenNotDropped() {
        Converted converted = SqliteFixture.convert(
                CorpusFile.get("jackcess/V2019/extDateV2019.accdb").file(), dir.resolve("extDate.sqlite3"));
        try (Sqlite sqlite = converted.open()) {
            // v2 wrote NULL for every Date/Time Extended value (F-10)
            assertThat(sqlite.value("SELECT count(*) FROM Table1 WHERE DateExt IS NOT NULL"))
                    .isEqualTo(8);
            assertThat(sqlite.value("SELECT DateExt FROM Table1 WHERE Field1 = 'row6'"))
                    .isEqualTo("2021-06-14 22:45:12.3456789");
            assertThat(sqlite.value("SELECT DateExt FROM Table1 WHERE Field1 = 'row8'"))
                    .isEqualTo("0100-06-14 12:45:00.1234567");
        }
    }

    @Test
    void oleAndBinaryValuesKeepTheirExactBytes() {
        try (Sqlite sqlite = schemaFidelity.open()) {
            assertThat(sqlite.value("SELECT hex(Raw) FROM Files WHERE FileID = 1"))
                    .isEqualTo("00112233445566778899AABBCCDDEEFF");
            // A raw PNG written by code has no OLE wrapper; v2 wrote NULL for it (F-11)
            assertThat(sqlite.value("SELECT hex(Img) FROM Files WHERE FileID = 1"))
                    .isEqualTo("89504E470D0A1A0A0000000D49484452");
            assertThat(sqlite.value("SELECT typeof(Doc) FROM Files WHERE FileID = 1"))
                    .isEqualTo("blob");
            assertThat(sqlite.value("SELECT Raw FROM Files WHERE FileID = 2")).isNull();
        }
    }

    @Test
    void aColumnJackcessCannotDecodeKeepsItsRawBytes() {
        Converted converted = SqliteFixture.convert(
                CorpusFile.get("jackcess/V2007/unsupportedFieldsV2007.accdb").file(),
                dir.resolve("unsupported.sqlite3"),
                CorpusFile.get("jackcess/V2007/unsupportedFieldsV2007.accdb").openOptions(),
                ConvertOptions.DEFAULT,
                SqliteOptions.DEFAULT);
        try (Sqlite sqlite = converted.open()) {
            assertThat(sqlite.value("SELECT hex(UnknownVar) FROM Test WHERE ID = 1"))
                    .isEqualTo("FFFE736F6D6564617461");
            assertThat(sqlite.value("SELECT hex(UnknownFix) FROM Test WHERE ID = 1"))
                    .isEqualTo("37000000");
        }
        assertThat(converted.issues(IssueCode.UNSUPPORTED_COLUMN_TYPE)).hasSize(3);
    }

    @Test
    void textKeepsEveryCharacterItHadInAccess() {
        Converted converted = SqliteFixture.convert(GeneratedFixture.TEXT_EDGE.path(), dir.resolve("textEdge.sqlite3"));
        try (Sqlite sqlite = converted.open()) {
            // Backslashes, quotes, newlines and emoji: v2's MySQL dump broke on these (F-02)
            assertThat(sqlite.value("SELECT Val FROM TextEdge WHERE Label = 'windows-path'"))
                    .isEqualTo("C:\\temp\\new");
            assertThat(sqlite.value("SELECT Val FROM TextEdge WHERE Label = 'quotes'"))
                    .isEqualTo("single ' and double \"");
            assertThat(sqlite.value("SELECT Val FROM TextEdge WHERE Label = 'emoji'"))
                    .isEqualTo("emoji \uD83D\uDE00 and \u03A9\u03BC\u03AD\u03B3\u03B1");
        }
    }
}
