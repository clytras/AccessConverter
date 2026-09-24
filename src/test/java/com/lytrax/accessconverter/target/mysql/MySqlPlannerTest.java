package com.lytrax.accessconverter.target.mysql;

import static com.lytrax.accessconverter.model.Models.column;
import static com.lytrax.accessconverter.model.Models.foreignKey;
import static com.lytrax.accessconverter.model.Models.index;
import static com.lytrax.accessconverter.model.Models.primaryKey;
import static com.lytrax.accessconverter.model.Models.schema;
import static com.lytrax.accessconverter.model.Models.table;
import static com.lytrax.accessconverter.model.Models.unique;
import static com.lytrax.accessconverter.profile.Profiles.profile;
import static com.lytrax.accessconverter.profile.Profiles.stats;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

import com.lytrax.accessconverter.model.AccessType;
import com.lytrax.accessconverter.model.ForeignKeyModel.Action;
import com.lytrax.accessconverter.model.Models;
import com.lytrax.accessconverter.model.SchemaModel;
import com.lytrax.accessconverter.profile.DataProfile;
import com.lytrax.accessconverter.report.IssueCode;
import com.lytrax.accessconverter.report.Issues;
import com.lytrax.accessconverter.target.BinaryMode;
import com.lytrax.accessconverter.target.ComplexTables;
import com.lytrax.accessconverter.target.ConvertOptions;
import com.lytrax.accessconverter.target.mysql.MySqlPlan.KeyPart;
import com.lytrax.accessconverter.target.mysql.MySqlPlan.PlannedColumn;
import com.lytrax.accessconverter.target.mysql.MySqlPlan.PlannedIndex;
import com.lytrax.accessconverter.target.mysql.MySqlPlan.PlannedTable;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * The planner's rules from 05, on hand-built models: what no corpus database holds (a key over InnoDB's limit, a
 * CHECK MySQL refuses beside a foreign key's action, text a collation compares unlike Access) is decided here.
 */
class MySqlPlannerTest {

    private static final ConvertOptions NO_PROFILE =
            new ConvertOptions(false, false, ConvertOptions.OnTableError.FAIL, 1000);

    private final Issues issues = new Issues();

    @Test
    void typesFollowAccessExactly() {
        SchemaModel model = schema(table(
                "T",
                primaryKey("Id"),
                column("Id", AccessType.AUTONUMBER_LONG),
                column("Flag", AccessType.BOOLEAN),
                column("Rating", AccessType.BYTE),
                column("Small", AccessType.INT),
                column("Large", AccessType.BIG_INT),
                column("Single", AccessType.FLOAT),
                column("Real", AccessType.DOUBLE),
                column("Price", AccessType.MONEY),
                column("Ratio", AccessType.NUMERIC).decimal(10, 3),
                column("Day", AccessType.SHORT_DATE_TIME),
                column("Stamp", AccessType.SHORT_DATE_TIME),
                column("Precise", AccessType.EXT_DATE_TIME),
                column("Code", AccessType.TEXT).length(50),
                column("Notes", AccessType.MEMO),
                column("Link", AccessType.HYPERLINK),
                column("Guid", AccessType.GUID),
                column("Replica", AccessType.AUTONUMBER_GUID),
                column("Raw", AccessType.BINARY).length(16),
                column("Picture", AccessType.OLE)));
        DataProfile data = profile()
                .table("T", 1, stats("Day").fractionDigits(0), stats("Stamp").fractionDigits(2))
                .build();
        PlannedTable t = plan(model, data, MySqlDialect.MYSQL).table("T").orElseThrow();

        assertThat(t.columns().stream().map(PlannedColumn::type).toList())
                .containsExactly(
                        "INT",
                        "BOOLEAN",
                        "TINYINT UNSIGNED",
                        "SMALLINT",
                        "BIGINT",
                        "FLOAT",
                        "DOUBLE",
                        "DECIMAL(19,4)",
                        "DECIMAL(10,3)",
                        "DATETIME",
                        "DATETIME(3)",
                        "DATETIME(6)",
                        "VARCHAR(50)",
                        "LONGTEXT",
                        "LONGTEXT",
                        "CHAR(38)",
                        "CHAR(38)",
                        "VARBINARY(16)",
                        "LONGBLOB");
        assertThat(planned(t, "Id").autoIncrement()).isTrue();
        assertThat(planned(t, "Id").notNull()).isTrue();
        // Access's Yes/No can't hold NULL, and its default is No
        assertThat(planned(t, "Flag").notNull()).isTrue();
        assertThat(planned(t, "Flag").defaultSql()).isEqualTo("0");
        assertThat(planned(t, "Replica").defaultSql()).isEqualTo(MySqlExpressions.RANDOM_GUID);
        assertThat(t.createTableSql()).contains("`Flag` BOOLEAN NOT NULL DEFAULT 0");
    }

    @Test
    void anAutonumberThatIsNotThePrimaryKeyGetsItsKeyInCreateTable() {
        SchemaModel model = schema(
                table(
                        "Shippers",
                        primaryKey("Code"),
                        column("Id", AccessType.AUTONUMBER_LONG),
                        column("Code", AccessType.TEXT)),
                table(
                        "Carriers",
                        primaryKey("Code"),
                        List.of(index("ByNumber", "Number", "Code")),
                        column("Number", AccessType.AUTONUMBER_LONG),
                        column("Code", AccessType.TEXT)));
        MySqlPlan plan = plan(model, profile().build(), MySqlDialect.MYSQL);

        // F-35: InnoDB needs an index that starts with the AUTO_INCREMENT column when the table is created
        PlannedTable shippers = plan.table("Shippers").orElseThrow();
        assertThat(shippers.inlineIndexes())
                .singleElement()
                .satisfies(i -> assertThat(i.columnNames()).containsExactly("Id"));
        assertThat(shippers.createTableSql()).contains("KEY `Id` (`Id`)");
        assertThat(issues.list()).anyMatch(i -> i.code() == IssueCode.INDEX_ADDED_FOR_AUTO_INCREMENT);
        // An Access index that already starts with it moves into CREATE TABLE instead of adding another
        PlannedTable carriers = plan.table("Carriers").orElseThrow();
        assertThat(carriers.inlineIndexes()).extracting(PlannedIndex::name).containsExactly("ByNumber");
        assertThat(carriers.indexes()).isEmpty();
    }

    @Test
    void aRandomAutonumberGetsARandomDefaultAndSaysHowOftenAValueIsTaken() {
        SchemaModel model = schema(
                table(
                        "Random",
                        primaryKey("Id"),
                        column("Id", AccessType.AUTONUMBER_LONG).defaultValue("GenUniqueID()")),
                table("Increment", primaryKey("Id"), column("Id", AccessType.AUTONUMBER_LONG)));
        MySqlPlan plan = plan(
                model,
                profile()
                        .table("Random", 1_000_000, stats("Id").maxAutoNumber(2_147_483_647L))
                        .build(),
                MySqlDialect.MYSQL);

        // GenUniqueID() is the Random setting: a random DEFAULT, no AUTO_INCREMENT, and no untranslatable default
        PlannedColumn random = planned(plan.table("Random").orElseThrow(), "Id");
        assertThat(random.defaultSql()).isEqualTo(MySqlExpressions.RANDOM_INT);
        assertThat(random.autoIncrement()).isFalse();
        assertThat(plan.table("Random").orElseThrow().autoIncrementSeed()).isNull();
        assertThat(planned(plan.table("Increment").orElseThrow(), "Id").autoIncrement())
                .isTrue();
        assertThat(issues.list()).noneMatch(i -> i.code() == IssueCode.DEFAULT_UNTRANSLATABLE);
        assertThat(issues.list()).noneMatch(i -> i.code() == IssueCode.AUTONUMBER_RANDOM_SEQUENTIAL);
        assertThat(issues.list())
                .filteredOn(i -> i.code() == IssueCode.AUTONUMBER_RANDOM_DEFAULT)
                .singleElement()
                .satisfies(i -> {
                    assertThat(i.table()).isEqualTo("Random");
                    assertThat(i.message())
                            .contains(
                                    "no ceiling",
                                    "LAST_INSERT_ID() and the client calls built on it give 0",
                                    "must select the row instead",
                                    "with the table's 1,000,000 rows, about one insert in 4,295 draws a value that is"
                                            + " taken",
                                    "kept exactly");
                });

        Issues unprofiled = new Issues();
        MySqlPlanner.plan(model, null, NO_PROFILE, MySqlOptions.of(MySqlDialect.MARIADB), unprofiled);
        assertThat(unprofiled.list())
                .filteredOn(i -> i.code() == IssueCode.AUTONUMBER_RANDOM_DEFAULT)
                .singleElement()
                .satisfies(i -> assertThat(i.message()).contains("without a profile the row count"));
    }

    @Test
    void aUniqueKeyOverInnoDbsLimitBecomesAPlainIndexWithPrefixes() {
        SchemaModel model = schema(table(
                "T",
                primaryKey("Id"),
                List.of(unique("Wide", "A", "B", "C", "D"), index("Plain", "A", "B", "C", "N"), index("Memo", "M")),
                column("Id", AccessType.LONG),
                column("A", AccessType.TEXT),
                column("B", AccessType.TEXT),
                column("C", AccessType.TEXT),
                column("D", AccessType.TEXT).length(10),
                column("N", AccessType.LONG),
                column("M", AccessType.MEMO)));
        PlannedTable t =
                plan(model, profile().build(), MySqlDialect.MYSQL).table("T").orElseThrow();

        PlannedIndex wide = plannedIndex(t, "Wide");
        // A prefix would make it reject values Access accepts, so it stops being unique
        assertThat(wide.unique()).isFalse();
        assertThat(keyBytes(t, wide)).isLessThanOrEqualTo(MySqlPlanner.MAX_KEY_BYTES);
        // The short column stays whole; the long ones share what is left
        assertThat(wide.parts().get(3).prefix()).isNull();
        assertThat(issues.list())
                .filteredOn(i -> i.code() == IssueCode.UNIQUE_DOWNGRADED_KEY_TOO_LONG)
                .extracting(i -> i.object())
                .containsExactly("Wide");
        PlannedIndex plain = plannedIndex(t, "Plain");
        assertThat(keyBytes(t, plain)).isLessThanOrEqualTo(MySqlPlanner.MAX_KEY_BYTES);
        // Access indexes the first 255 characters of a Long Text
        assertThat(plannedIndex(t, "Memo").parts()).containsExactly(new KeyPart("M", true, 255));
    }

    @Test
    void wideTablesWidenTheirLargestUnindexedVarcharsToText() {
        List<Models.Column> columns = new ArrayList<>();
        columns.add(column("Id", AccessType.LONG));
        for (int i = 1; i <= 70; i++) {
            columns.add(column("T" + i, AccessType.TEXT));
        }
        SchemaModel model = schema(
                table("Wide", primaryKey("Id"), List.of(index("ByT1", "T1")), columns.toArray(Models.Column[]::new)));
        PlannedTable t =
                plan(model, profile().build(), MySqlDialect.MYSQL).table("Wide").orElseThrow();

        // F-18: 70 x VARCHAR(255) is over 65,535 bytes; the indexed T1 stays a VARCHAR
        assertThat(planned(t, "T1").type()).isEqualTo("VARCHAR(255)");
        List<String> widened = t.columns().stream()
                .filter(c -> c.type().equals("TEXT"))
                .map(PlannedColumn::name)
                .toList();
        assertThat(widened).containsExactly("T2", "T3", "T4", "T5", "T6", "T7");
        assertThat(issues.list())
                .filteredOn(i -> i.code() == IssueCode.TEXT_WIDENED_ROW_SIZE)
                .hasSize(6);
    }

    @Test
    void aRowStaysWithinWhatAnInnoDbPageHoldsInline() {
        SchemaModel model = textColumns(180);
        // MySQL 8.0 and MariaDB reject the table as it is (ERROR 1118, > 8126, measured); 8.4 accepts it
        for (MySqlDialect dialect : MySqlDialect.values()) {
            PlannedTable t = plan(model, profile().build(), dialect).table("T").orElseThrow();
            long varchars = t.columns().stream()
                    .filter(c -> c.type().startsWith("VARCHAR"))
                    .count();
            long nullable = t.columns().stream().filter(c -> !c.notNull()).count();
            // 5 + NULL bitmap + 13 system bytes + INT + each VARCHAR(50) at 201 bytes + each TEXT at 21 (MariaDB) or
            // 41 (MySQL 8.0)
            long text = dialect == MySqlDialect.MARIADB ? 21 : 41;
            long inline = 5 + (nullable + 7) / 8 + 13 + 4 + varchars * 201 + (180 - varchars) * text;
            assertThat(inline).as("%s", dialect).isLessThanOrEqualTo(MySqlPlanner.MAX_INLINE_BYTES);
            assertThat(varchars).isGreaterThan(0);
        }
        assertThat(issues.list()).noneMatch(i -> i.code() == IssueCode.TABLE_ROW_TOO_LARGE);
    }

    @Test
    void aRowNoWideningCanFitIsReported() {
        // 254 long columns are 254 x 41 bytes inline on MySQL 8.0 whatever their type: only 8.4 creates the table
        plan(textColumns(254), profile().build(), MySqlDialect.MYSQL);
        assertThat(issues.list())
                .filteredOn(i -> i.code() == IssueCode.TABLE_ROW_TOO_LARGE)
                .singleElement()
                .satisfies(i -> assertThat(i.message()).contains("MySQL 8.4 creates it"));
    }

    private static SchemaModel textColumns(int count) {
        List<Models.Column> columns = new ArrayList<>();
        columns.add(column("Id", AccessType.LONG));
        for (int i = 1; i <= count; i++) {
            columns.add(column("C" + i, AccessType.TEXT).length(50));
        }
        return schema(table("T", primaryKey("Id"), columns.toArray(Models.Column[]::new)));
    }

    @Test
    void aCheckMySqlRefusesBesideAnAutoIncrementOrAForeignKeyActionIsLeftOut() {
        SchemaModel model = schema(
                List.of(
                        table("Parent", primaryKey("Id"), column("Id", AccessType.LONG)),
                        table(
                                "Child",
                                primaryKey("Id"),
                                List.of(index("ByUpdated", "Updated"), index("ByDeleted", "Deleted")),
                                column("Id", AccessType.AUTONUMBER_LONG).validation(">0"),
                                column("Updated", AccessType.LONG).validation(">0"),
                                column("Deleted", AccessType.LONG).validation(">0"))),
                List.of(
                        foreignKey(
                                "Upd",
                                "Parent",
                                "Id",
                                "Child",
                                "Updated",
                                "PrimaryKey",
                                Action.CASCADE,
                                Action.NO_ACTION),
                        foreignKey(
                                "Del",
                                "Parent",
                                "Id",
                                "Child",
                                "Deleted",
                                "PrimaryKey",
                                Action.NO_ACTION,
                                Action.CASCADE)));
        DataProfile data = profile()
                .table(
                        "Child",
                        1,
                        stats("Id").rule(0, 0),
                        stats("Updated").rule(0, 0),
                        stats("Deleted").rule(0, 0))
                .relationship("Upd", 1, 0)
                .relationship("Del", 1, 0)
                .build();
        PlannedTable child =
                plan(model, data, MySqlDialect.MYSQL).table("Child").orElseThrow();

        // ON DELETE CASCADE deletes rows and leaves the column alone, so its CHECK stays (measured)
        assertThat(child.checks())
                .singleElement()
                .satisfies(c -> assertThat(c.column()).isEqualTo("Deleted"));
        assertThat(child.foreignKeys()).hasSize(2);
        assertThat(issues.list())
                .filteredOn(i -> i.code() == IssueCode.CHECK_UNTRANSLATABLE)
                .extracting(i -> i.object())
                .containsExactlyInAnyOrder("Id", "Updated");
        assertThat(planned(child, "Updated").comment()).contains("Access validation rule: >0");
    }

    @Test
    void setNullOnARequiredChildBecomesNoAction() {
        SchemaModel model = schema(
                List.of(
                        table("Parent", primaryKey("Id"), column("Id", AccessType.LONG)),
                        table(
                                "Child",
                                primaryKey("Id"),
                                List.of(index("ByParent", "ParentId")),
                                column("Id", AccessType.LONG),
                                column("ParentId", AccessType.LONG).required())),
                List.of(foreignKey(
                        "Rel", "Parent", "Id", "Child", "ParentId", "PrimaryKey", Action.NO_ACTION, Action.SET_NULL)));
        DataProfile data = profile()
                .table("Child", 1, stats("ParentId").nulls(0))
                .relationship("Rel", 1, 0)
                .build();
        PlannedTable child =
                plan(model, data, MySqlDialect.MYSQL).table("Child").orElseThrow();

        assertThat(child.foreignKeys())
                .singleElement()
                .satisfies(fk -> assertThat(fk.onDelete()).isEqualTo(Action.NO_ACTION));
        assertThat(issues.list()).anyMatch(i -> i.code() == IssueCode.FK_SET_NULL_ON_REQUIRED);
    }

    @Test
    void aForeignKeyNeedsItsParentKeyStillUniqueInTheOutput() {
        SchemaModel model = schema(
                List.of(
                        table(
                                "Parent",
                                primaryKey("Id"),
                                List.of(unique("ByKey", "A", "B", "C", "D")),
                                column("Id", AccessType.LONG),
                                column("A", AccessType.TEXT),
                                column("B", AccessType.TEXT),
                                column("C", AccessType.TEXT),
                                column("D", AccessType.TEXT)),
                        table("Child", null, column("A", AccessType.TEXT))),
                List.of());
        SchemaModel withFk = schema(
                model.tables(),
                List.of(new com.lytrax.accessconverter.model.ForeignKeyModel(
                        "Rel",
                        "Parent",
                        List.of("A", "B", "C", "D"),
                        "Child",
                        List.of("A", "A", "A", "A"),
                        true,
                        Action.NO_ACTION,
                        Action.NO_ACTION,
                        false,
                        com.lytrax.accessconverter.model.ForeignKeyModel.Join.INNER,
                        0,
                        com.lytrax.accessconverter.model.ForeignKeyModel.Status.EMIT,
                        "ByKey")));
        MySqlPlan plan = plan(withFk, profile().relationship("Rel", 1, 0).build(), MySqlDialect.MYSQL);

        assertThat(plan.table("Child").orElseThrow().foreignKeys()).isEmpty();
        assertThat(issues.list()).anyMatch(i -> i.code() == IssueCode.FK_SKIPPED_PARENT_KEY_MISSING);
    }

    @Test
    void defaultsAreWrittenOnlyWhereTheyFit() {
        SchemaModel model = schema(table(
                "T",
                primaryKey("Id"),
                column("Id", AccessType.LONG),
                column("Short", AccessType.TEXT).length(3).defaultValue("\"abcdef\""),
                column("Name", AccessType.TEXT).length(20).defaultValue("\"it's\""),
                column("Notes", AccessType.MEMO).defaultValue("\"n/a\""),
                column("Created", AccessType.SHORT_DATE_TIME).defaultValue("Now()"),
                column("Today", AccessType.SHORT_DATE_TIME).defaultValue("Date()"),
                column("Clock", AccessType.SHORT_DATE_TIME).defaultValue("Time()"),
                column("Rating", AccessType.BYTE).defaultValue("300"),
                column("Active", AccessType.BOOLEAN).defaultValue("Yes"),
                column("Due", AccessType.SHORT_DATE_TIME).defaultValue("#1/31/2000#"),
                column("Raw", AccessType.BINARY).length(4).defaultValue("0")));
        DataProfile data = profile()
                .table(
                        "T",
                        1,
                        stats("Created").fractionDigits(3),
                        stats("Today").fractionDigits(0),
                        stats("Clock").fractionDigits(0),
                        stats("Due").fractionDigits(0))
                .build();
        PlannedTable t = plan(model, data, MySqlDialect.MYSQL).table("T").orElseThrow();

        assertThat(planned(t, "Short").defaultSql()).isNull();
        assertThat(planned(t, "Name").defaultSql()).isEqualTo("'it\\'s'");
        // MySQL rejects a literal default on TEXT (error 1101); an expression is accepted
        assertThat(planned(t, "Notes").defaultSql()).isEqualTo("('n/a')");
        // The precision must match the column's, or the server rejects it
        assertThat(planned(t, "Created").defaultSql()).isEqualTo("CURRENT_TIMESTAMP(3)");
        assertThat(planned(t, "Today").defaultSql()).isEqualTo("(CURRENT_DATE)");
        assertThat(planned(t, "Clock").defaultSql()).isEqualTo("(TIMESTAMP('1899-12-30', CURRENT_TIME))");
        assertThat(planned(t, "Rating").defaultSql()).isNull();
        assertThat(planned(t, "Active").defaultSql()).isEqualTo("1");
        assertThat(planned(t, "Due").defaultSql()).isEqualTo("'2000-01-31 00:00:00'");
        assertThat(planned(t, "Raw").defaultSql()).isNull();
        assertThat(issues.list())
                .filteredOn(i -> i.code() == IssueCode.DEFAULT_DROPPED_TYPE_MISMATCH)
                .extracting(i -> i.object())
                .containsExactlyInAnyOrder("Short", "Rating", "Raw");
        assertThat(planned(t, "Short").comment()).isEqualTo("Access default: \"abcdef\"");
    }

    @Test
    void keyTextTheCollationComparesUnlikeAccessIsComparedInBinary() {
        SchemaModel model = schema(
                List.of(
                        table(
                                "Parent",
                                primaryKey("Code"),
                                List.of(unique("ByName", "Name")),
                                column("Code", AccessType.TEXT).length(10),
                                column("Name", AccessType.TEXT),
                                column("Note", AccessType.TEXT)),
                        table(
                                "Child",
                                null,
                                List.of(index("ByCode", "Code")),
                                column("Code", AccessType.TEXT).length(10))),
                List.of(foreignKey(
                        "Rel", "Parent", "Code", "Child", "Code", "PrimaryKey", Action.NO_ACTION, Action.NO_ACTION)));
        DataProfile data = profile()
                .table("Parent", 2, stats("Code").unsafeKeyText(1))
                .relationship("Rel", 1, 0)
                .build();
        MySqlPlan plan = plan(model, data, MySqlDialect.MARIADB);

        PlannedTable parent = plan.table("Parent").orElseThrow();
        assertThat(planned(parent, "Code").collation()).isEqualTo("utf8mb4_bin");
        assertThat(parent.createTableSql()).contains("`Code` VARCHAR(10) COLLATE utf8mb4_bin NOT NULL");
        // Only the column that holds such text; a foreign key's other side follows it
        assertThat(planned(parent, "Name").collation()).isNull();
        assertThat(planned(plan.table("Child").orElseThrow(), "Code").collation())
                .isEqualTo("utf8mb4_bin");
        assertThat(issues.list())
                .filteredOn(i -> i.code() == IssueCode.COLLATION_BINARY_KEY)
                .extracting(i -> i.table() + "." + i.object())
                .containsExactlyInAnyOrder("Parent.Code", "Child.Code");

        // Without a profile nothing is known, so every key text is compared in binary
        Issues unprofiled = new Issues();
        MySqlPlan conservative =
                MySqlPlanner.plan(model, null, NO_PROFILE, MySqlOptions.of(MySqlDialect.MARIADB), unprofiled);
        assertThat(planned(conservative.table("Parent").orElseThrow(), "Name").collation())
                .isEqualTo("utf8mb4_bin");
        assertThat(planned(conservative.table("Parent").orElseThrow(), "Note").collation())
                .isNull();
        // An explicit --collation is the user's choice
        MySqlPlan chosen = MySqlPlanner.plan(
                model,
                data,
                ConvertOptions.DEFAULT,
                new MySqlOptions(MySqlDialect.MYSQL, "utf8mb4_bin", false, null, 1 << 20, false),
                new Issues());
        assertThat(planned(chosen.table("Parent").orElseThrow(), "Code").collation())
                .isNull();
    }

    @Test
    void aCaseOnlyForeignKeyMatchNeedsACaseInsensitiveCollation() {
        SchemaModel model = schema(
                List.of(
                        table(
                                "Parent",
                                primaryKey("Code"),
                                column("Code", AccessType.TEXT).length(10)),
                        table(
                                "Child",
                                null,
                                List.of(index("ByCode", "Code")),
                                column("Code", AccessType.TEXT).length(10))),
                List.of(foreignKey(
                        "Rel", "Parent", "Code", "Child", "Code", "PrimaryKey", Action.NO_ACTION, Action.NO_ACTION)));
        DataProfile data = profile().relationship("Rel", 2, 0, 1, true).build();

        assertThat(plan(model, data, MySqlDialect.MYSQL)
                        .table("Child")
                        .orElseThrow()
                        .foreignKeys())
                .hasSize(1);
        MySqlPlan binary = MySqlPlanner.plan(
                model,
                data,
                ConvertOptions.DEFAULT,
                new MySqlOptions(MySqlDialect.MYSQL, "utf8mb4_bin", false, null, 1 << 20, false),
                issues);
        assertThat(binary.table("Child").orElseThrow().foreignKeys()).isEmpty();
        assertThat(issues.list()).anyMatch(i -> i.code() == IssueCode.FK_SKIPPED_ORPHANS);
    }

    @Test
    void aCalculatedDecimalKeepsTheScaleItsValuesHave() {
        SchemaModel model = schema(table(
                "T",
                primaryKey("Id"),
                column("Id", AccessType.LONG),
                column("Weekly", AccessType.NUMERIC).decimal(28, 0).calculated("[Salary]/52")));
        DataProfile data =
                profile().table("T", 1, stats("Weekly").digits(15, 16)).build();

        // calcFieldV2010: declared (28,0), holds 16 fractional digits; (28,0) would round them (note 1265)
        assertThat(planned(plan(model, data, MySqlDialect.MYSQL).table("T").orElseThrow(), "Weekly")
                        .type())
                .isEqualTo("DECIMAL(44,16)");
        assertThat(issues.list()).anyMatch(i -> i.code() == IssueCode.DECIMAL_SCALE_WIDENED);
    }

    @Test
    void commentsFitWhatMySqlKeeps() {
        SchemaModel model = schema(table(
                "T",
                primaryKey("Id"),
                column("Id", AccessType.LONG).description("x".repeat(1100)),
                column("Mood", AccessType.TEXT).description("happy \uD83D\uDE00")));
        PlannedTable t =
                plan(model, profile().build(), MySqlDialect.MYSQL).table("T").orElseThrow();

        assertThat(planned(t, "Id").comment()).hasSize(MySqlPlanner.MAX_COLUMN_COMMENT);
        // Comments are utf8mb3 in the data dictionary, so an emoji would become '?'
        assertThat(planned(t, "Mood").comment()).isEqualTo("happy \uFFFD");
        assertThat(issues.list())
                .filteredOn(i -> i.code() == IssueCode.COMMENT_TRUNCATED)
                .hasSize(2);
    }

    // ---------------------------------------------------------------- helpers

    @Test
    void complexColumnsBecomeChildTablesReferringToTheirUniqueComplexId() {
        SchemaModel model = ComplexTables.expand(
                schema(table(
                        "T",
                        primaryKey("Id"),
                        List.of(unique("files_index", "Files"), unique("tags_index", "Tags")),
                        column("Id", AccessType.AUTONUMBER_LONG),
                        column("Files", AccessType.ATTACHMENT),
                        Models.column("Tags", AccessType.MULTI_VALUE).element(AccessType.INT))),
                ConvertOptions.DEFAULT);
        MySqlPlan plan =
                MySqlPlanner.plan(model, null, ConvertOptions.DEFAULT, MySqlOptions.of(MySqlDialect.MARIADB), issues);

        PlannedTable parent = plan.table("T").orElseThrow();
        assertThat(parent.column("Files").orElseThrow().type()).isEqualTo("INT");
        assertThat(parent.indexes())
                .extracting(PlannedIndex::name, PlannedIndex::unique)
                .contains(tuple("files_index", true), tuple("tags_index", true));
        assertThat(plan.table("T_Files").orElseThrow().createTableSql())
                .contains(
                        "`id` INT NOT NULL", "`Files_ref` INT NOT NULL", "`file_data` LONGBLOB", "PRIMARY KEY (`id`)");
        assertThat(plan.table("T_Tags")
                        .orElseThrow()
                        .column("value")
                        .orElseThrow()
                        .type())
                .isEqualTo("SMALLINT");
        assertThat(plan.table("T_Tags").orElseThrow().foreignKeySql())
                .isEqualTo("ALTER TABLE `T_Tags`\n  ADD CONSTRAINT `T_Tags` FOREIGN KEY (`Tags_ref`) REFERENCES `T`"
                        + " (`Tags`) ON DELETE CASCADE ON UPDATE CASCADE");
        assertThat(issues.list()).noneMatch(i -> i.code() == IssueCode.INDEX_SKIPPED_COMPLEX_COLUMN);
    }

    @Test
    void binaryModesAndOleCompanionsDecideTheColumnTypes() {
        SchemaModel model = schema(table(
                "T",
                primaryKey("Id"),
                column("Id", AccessType.LONG),
                column("Bin", AccessType.BINARY).length(16),
                Models.column("Ole", AccessType.OLE).defaultValue("\"x\"")));
        for (BinaryMode mode : BinaryMode.values()) {
            ConvertOptions options = ConvertOptions.DEFAULT.withBinary(mode, true, false);
            PlannedTable table = MySqlPlanner.plan(
                            model, null, options, MySqlOptions.of(MySqlDialect.MYSQL), new Issues())
                    .table("T")
                    .orElseThrow();
            String bytes =
                    switch (mode) {
                        case INLINE -> "LONGBLOB";
                        case FILES -> "VARCHAR(1024)";
                        case OMIT -> "BIGINT";
                    };
            assertThat(table.columns())
                    .extracting(PlannedColumn::name, PlannedColumn::type)
                    .as(mode.label())
                    .containsExactly(
                            tuple("Id", "INT"),
                            tuple("Bin", mode == BinaryMode.INLINE ? "VARBINARY(16)" : bytes),
                            tuple("Ole", bytes),
                            tuple("Ole__kind", "VARCHAR(16)"),
                            tuple("Ole__name", "LONGTEXT"),
                            tuple("Ole__mime", "VARCHAR(255)"),
                            tuple("Ole__content", bytes));
            assertThat(table.column("Ole").orElseThrow().defaultSql()).isNull();
        }
    }

    private MySqlPlan plan(SchemaModel model, DataProfile data, MySqlDialect dialect) {
        return MySqlPlanner.plan(model, data, ConvertOptions.DEFAULT, MySqlOptions.of(dialect), issues);
    }

    private static PlannedColumn planned(PlannedTable table, String name) {
        return table.column(name).orElseThrow(() -> new AssertionError("no column " + name));
    }

    private static PlannedIndex plannedIndex(PlannedTable table, String name) {
        return table.allIndexes().stream()
                .filter(i -> i.name().equals(name))
                .findFirst()
                .orElseThrow(() -> new AssertionError("no index " + name));
    }

    /** A key's bytes as InnoDB counts them: 4 per character of text, 4 for an INT. */
    private static long keyBytes(PlannedTable table, PlannedIndex index) {
        long bytes = 0;
        for (KeyPart part : index.parts()) {
            String type = planned(table, part.column()).type();
            if (part.prefix() != null) {
                bytes += 4L * part.prefix();
            } else if (type.startsWith("VARCHAR(")) {
                bytes += 4L * Integer.parseInt(type.substring(8, type.length() - 1));
            } else {
                bytes += 4;
            }
        }
        return bytes;
    }
}
