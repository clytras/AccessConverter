package com.lytrax.accessconverter.target.sqlite;

import static com.lytrax.accessconverter.model.Models.column;
import static com.lytrax.accessconverter.model.Models.foreignKey;
import static com.lytrax.accessconverter.model.Models.index;
import static com.lytrax.accessconverter.model.Models.notEnforced;
import static com.lytrax.accessconverter.model.Models.primaryKey;
import static com.lytrax.accessconverter.model.Models.schema;
import static com.lytrax.accessconverter.model.Models.table;
import static com.lytrax.accessconverter.model.Models.unique;
import static com.lytrax.accessconverter.model.Models.uniqueIgnoringNulls;
import static com.lytrax.accessconverter.profile.Profiles.profile;
import static com.lytrax.accessconverter.profile.Profiles.stats;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

import com.lytrax.accessconverter.model.AccessType;
import com.lytrax.accessconverter.model.ForeignKeyModel.Action;
import com.lytrax.accessconverter.model.SchemaModel;
import com.lytrax.accessconverter.profile.DataProfile;
import com.lytrax.accessconverter.report.IssueCode;
import com.lytrax.accessconverter.report.Issues;
import com.lytrax.accessconverter.target.ComplexTables;
import com.lytrax.accessconverter.target.ConvertOptions;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * The planner's rules from 06, on hand-built models: what no corpus database holds (NULLs in a Required column, a
 * default that doesn't fit, an Ignore-Nulls parent key, a case-only foreign key match) is decided here.
 */
class SqlitePlannerTest {

    private final Issues issues = new Issues();

    @Test
    void notNullOnlyWhereTheDataAllowsIt() {
        SchemaModel model = schema(table(
                "T",
                primaryKey("Id"),
                column("Id", AccessType.AUTONUMBER_LONG),
                column("Code", AccessType.TEXT).required(),
                column("Name", AccessType.TEXT).required(),
                column("Flag", AccessType.BOOLEAN),
                column("Note", AccessType.MEMO)));
        DataProfile data = profile()
                .table(
                        "T",
                        3,
                        stats("Id").nulls(0).maxAutoNumber(7),
                        stats("Code").nulls(0),
                        // Access marks Name as required, but three rows hold NULL: NOT NULL would reject them
                        stats("Name").nulls(3))
                .build();
        SqlitePlan plan = plan(model, data);

        assertThat(planned(plan, "T", "Id").notNull()).isTrue();
        assertThat(planned(plan, "T", "Code").notNull()).isTrue();
        assertThat(planned(plan, "T", "Name").notNull()).isFalse();
        // Access's Yes/No can't hold NULL, and its default is No
        assertThat(planned(plan, "T", "Flag").notNull()).isTrue();
        assertThat(planned(plan, "T", "Flag").defaultSql()).isEqualTo("0");
        assertThat(planned(plan, "T", "Note").notNull()).isFalse();
        assertThat(issues.list())
                .filteredOn(i -> i.code() == IssueCode.NOT_NULL_DROPPED_NULLS_PRESENT)
                .singleElement()
                .satisfies(i -> assertThat(i.object()).isEqualTo("Name"));
    }

    @Test
    void withoutAProfileOnlyAccessesOwnGuaranteesBecomeNotNull() {
        SchemaModel model = schema(table(
                "T",
                primaryKey("Id"),
                column("Id", AccessType.LONG),
                column("Code", AccessType.TEXT).required(),
                column("Balance", AccessType.MONEY)));
        SqlitePlan plan = SqlitePlanner.plan(
                model,
                null,
                new ConvertOptions(false, false, ConvertOptions.OnTableError.FAIL, 1000),
                SqliteOptions.DEFAULT,
                issues);

        assertThat(planned(plan, "T", "Id").notNull()).isTrue();
        assertThat(planned(plan, "T", "Code").notNull()).isFalse();
        // Without the profile the digits are unknown, so the decimal is kept as exact text
        assertThat(planned(plan, "T", "Balance").declaredType()).isEqualTo("TEXT");
        assertThat(issues.list()).anySatisfy(i -> assertThat(i.code()).isEqualTo(IssueCode.PROFILE_SKIPPED));
    }

    @Test
    void theExactDecimalDecisionFollowsTheProfiledDigits() {
        SchemaModel model = schema(table(
                "T",
                primaryKey("Id"),
                column("Id", AccessType.AUTONUMBER_LONG),
                column("Small", AccessType.MONEY),
                column("Big", AccessType.MONEY),
                column("Scaled", AccessType.NUMERIC).decimal(28, 10)));
        DataProfile data = profile()
                .table(
                        "T",
                        1,
                        stats("Small").digits(6, 4),
                        stats("Big").digits(19, 4),
                        stats("Scaled").digits(12, 10))
                .build();
        SqlitePlan plan = plan(model, data);

        assertThat(planned(plan, "T", "Small").declaredType()).isEqualTo("DECIMAL(19,4)");
        assertThat(planned(plan, "T", "Small").form()).isEqualTo(SqlitePlan.ValueForm.DECIMAL_NUMBER);
        assertThat(planned(plan, "T", "Big").declaredType()).isEqualTo("TEXT");
        assertThat(planned(plan, "T", "Big").form()).isEqualTo(SqlitePlan.ValueForm.DECIMAL_TEXT);
        assertThat(planned(plan, "T", "Scaled").declaredType()).isEqualTo("DECIMAL(28,10)");
        assertThat(issues.list())
                .filteredOn(i -> i.code() == IssueCode.DECIMAL_STORED_AS_TEXT)
                .singleElement()
                .satisfies(i -> assertThat(i.object()).isEqualTo("Big"));
    }

    @Test
    void aDefaultThatDoesNotFitItsColumnIsDroppedAndKeptAsAComment() {
        SchemaModel model = schema(table(
                "T",
                primaryKey("Id"),
                column("Id", AccessType.AUTONUMBER_LONG),
                column("Count", AccessType.LONG).defaultValue("\"many\"")));
        SqlitePlan plan = plan(model, profile().table("T", 0).build());

        assertThat(planned(plan, "T", "Count").defaultSql()).isNull();
        assertThat(plan.table("T").orElseThrow().createTableSql()).contains("-- Access default: \"many\"");
        assertThat(issues.list())
                .filteredOn(i -> i.code() == IssueCode.DEFAULT_DROPPED_TYPE_MISMATCH)
                .singleElement()
                .satisfies(i -> assertThat(i.message()).contains("is not a number"));
    }

    @Test
    void aCheckIsOnlyEmittedWhenEveryRowComplies() {
        SchemaModel model = schema(table(
                "T",
                primaryKey("Id"),
                column("Id", AccessType.AUTONUMBER_LONG),
                column("Good", AccessType.LONG).validation(">0"),
                column("Bad", AccessType.LONG).validation(">0"),
                column("Code", AccessType.TEXT).noEmptyString(),
                column("Name", AccessType.TEXT).noEmptyString()));
        DataProfile data = profile()
                .table(
                        "T",
                        5,
                        stats("Good").rule(0, 0),
                        // Access doesn't re-check old rows when a rule is added, so the data can break its own rule
                        stats("Bad").rule(2, 0),
                        stats("Code").emptyStrings(0),
                        stats("Name").emptyStrings(3))
                .build();
        String sql = plan(model, data).table("T").orElseThrow().createTableSql();

        assertThat(sql).contains("CONSTRAINT \"ck_Good\" CHECK (\"Good\" > 0)");
        assertThat(sql).doesNotContain("ck_Bad");
        assertThat(sql).contains("CONSTRAINT \"ck_Code_nonempty\" CHECK (\"Code\" <> '')");
        assertThat(sql).doesNotContain("ck_Name_nonempty");
        assertThat(issues.list())
                .filteredOn(i -> i.code() == IssueCode.CHECK_VIOLATED_BY_DATA)
                .singleElement()
                .satisfies(i -> assertThat(i.object()).isEqualTo("Bad"));
    }

    @Test
    void aForeignKeyIsSkippedWhenTheDataHasOrphans() {
        SqlitePlan plan = plan(
                parentAndChild(),
                profile()
                        .table("Parent", 2, stats("Id").nulls(0))
                        .table("Child", 3)
                        .relationship("ParentChild", 3, 1)
                        .build());

        assertThat(plan.table("Child").orElseThrow().foreignKeys()).isEmpty();
        assertThat(issues.list())
                .filteredOn(i -> i.code() == IssueCode.FK_SKIPPED_ORPHANS)
                .singleElement()
                .satisfies(i -> assertThat(i.message()).contains("1 of 3 child rows have no parent"));
    }

    @Test
    void aTextForeignKeyThatOnlyMatchesCaseInsensitivelyGetsCollateNocase() {
        SchemaModel model = schema(
                List.of(
                        table(
                                "Parent",
                                primaryKey("Code"),
                                column("Code", AccessType.TEXT).length(10)),
                        table(
                                "Child",
                                primaryKey("Id"),
                                column("Id", AccessType.AUTONUMBER_LONG),
                                column("ParentCode", AccessType.TEXT).length(10))),
                List.of(foreignKey(
                        "ParentChild",
                        "Parent",
                        "Code",
                        "Child",
                        "ParentCode",
                        "PrimaryKey",
                        Action.NO_ACTION,
                        Action.NO_ACTION)));
        SqlitePlan plan = plan(
                model,
                profile()
                        .table("Parent", 2, stats("Code").nulls(0))
                        .table("Child", 3)
                        .relationship("ParentChild", 3, 0, 1, true)
                        .build());

        assertThat(planned(plan, "Parent", "Code").collateNocase()).isTrue();
        assertThat(planned(plan, "Child", "ParentCode").collateNocase()).isTrue();
        assertThat(plan.table("Child").orElseThrow().foreignKeys()).hasSize(1);
        assertThat(issues.list()).anySatisfy(i -> assertThat(i.code()).isEqualTo(IssueCode.FK_COLLATION_RELAXED));
    }

    @Test
    void aTextForeignKeyThatMatchesByMoreThanCaseIsSkipped() {
        SchemaModel model = schema(
                List.of(
                        table(
                                "Parent",
                                primaryKey("Code"),
                                column("Code", AccessType.TEXT).length(10)),
                        table(
                                "Child",
                                primaryKey("Id"),
                                column("Id", AccessType.AUTONUMBER_LONG),
                                column("ParentCode", AccessType.TEXT).length(10))),
                List.of(foreignKey(
                        "ParentChild",
                        "Parent",
                        "Code",
                        "Child",
                        "ParentCode",
                        "PrimaryKey",
                        Action.NO_ACTION,
                        Action.NO_ACTION)));
        SqlitePlan plan = plan(
                model,
                profile()
                        .table("Parent", 2, stats("Code").nulls(0))
                        .table("Child", 3)
                        .relationship("ParentChild", 3, 0, 1, false)
                        .build());

        assertThat(plan.table("Child").orElseThrow().foreignKeys()).isEmpty();
        assertThat(issues.list())
                .filteredOn(i -> i.code() == IssueCode.FK_SKIPPED_ORPHANS)
                .singleElement()
                .satisfies(i -> assertThat(i.message()).contains("more than ASCII letter case"));
    }

    @Test
    void setNullIsDowngradedWhenTheChildColumnCannotBeNull() {
        SchemaModel model = schema(
                List.of(
                        table("Parent", primaryKey("Id"), column("Id", AccessType.AUTONUMBER_LONG)),
                        table(
                                "Child",
                                primaryKey("Id"),
                                column("Id", AccessType.AUTONUMBER_LONG),
                                column("ParentId", AccessType.LONG).required())),
                List.of(foreignKey(
                        "ParentChild",
                        "Parent",
                        "Id",
                        "Child",
                        "ParentId",
                        "PrimaryKey",
                        Action.NO_ACTION,
                        Action.SET_NULL)));
        SqlitePlan plan = plan(
                model,
                profile()
                        .table("Parent", 1, stats("Id").nulls(0))
                        .table("Child", 1, stats("ParentId").nulls(0))
                        .relationship("ParentChild", 1, 0)
                        .build());

        assertThat(plan.table("Child").orElseThrow().foreignKeys())
                .singleElement()
                .satisfies(fk -> assertThat(fk.onDelete()).isEqualTo(Action.NO_ACTION));
        assertThat(plan.table("Child").orElseThrow().createTableSql()).doesNotContain("SET NULL");
        assertThat(issues.list()).anySatisfy(i -> assertThat(i.code()).isEqualTo(IssueCode.FK_SET_NULL_ON_REQUIRED));
    }

    @Test
    void aParentKeyIsNeverAPartialIndex() {
        SchemaModel model = schema(
                List.of(
                        table(
                                "Parent",
                                primaryKey("Id"),
                                List.of(uniqueIgnoringNulls("UX_Code", "Code"), uniqueIgnoringNulls("UX_Alt", "Alt")),
                                column("Id", AccessType.AUTONUMBER_LONG),
                                column("Code", AccessType.TEXT).length(10),
                                column("Alt", AccessType.TEXT).length(10)),
                        table(
                                "Child",
                                primaryKey("Id"),
                                column("Id", AccessType.AUTONUMBER_LONG),
                                column("ParentCode", AccessType.TEXT).length(10))),
                List.of(foreignKey(
                        "ParentChild",
                        "Parent",
                        "Code",
                        "Child",
                        "ParentCode",
                        "UX_Code",
                        Action.NO_ACTION,
                        Action.NO_ACTION)));
        SqlitePlan plan = plan(
                model,
                profile()
                        .table("Parent", 1, stats("Code").nulls(0))
                        .table("Child", 1)
                        .relationship("ParentChild", 1, 0)
                        .build());

        SqlitePlan.PlannedIndex key = indexOf(plan, "Parent", "Parent_UX_Code");
        // SQLite refuses a partial index as a foreign key's parent ("foreign key mismatch")
        assertThat(key.where()).isNull();
        assertThat(indexOf(plan, "Parent", "Parent_UX_Alt").where()).isEqualTo("\"Alt\" IS NOT NULL");
        assertThat(issues.list())
                .anySatisfy(i -> assertThat(i.code()).isEqualTo(IssueCode.PARTIAL_INDEX_DROPPED_PARENT_KEY));
    }

    @Test
    void ignoreNullsBecomesAPartialIndexOverEveryKeyColumn() {
        SchemaModel model = schema(table(
                "T",
                primaryKey("Id"),
                List.of(uniqueIgnoringNulls("UX_Pair", "A", "B DESC")),
                column("Id", AccessType.AUTONUMBER_LONG),
                column("A", AccessType.TEXT).length(10),
                column("B", AccessType.TEXT).length(10)));
        SqlitePlan plan = plan(model, profile().table("T", 0).build());

        SqlitePlan.PlannedIndex index = indexOf(plan, "T", "T_UX_Pair");
        // Access leaves a key out only when every column of it is NULL
        assertThat(index.where()).isEqualTo("\"A\" IS NOT NULL OR \"B\" IS NOT NULL");
        assertThat(index.createIndexSql())
                .isEqualTo("CREATE UNIQUE INDEX \"T_UX_Pair\" ON \"T\" (\"A\", \"B\" DESC)"
                        + " WHERE \"A\" IS NOT NULL OR \"B\" IS NOT NULL");
    }

    @Test
    void aRelationshipWithoutReferentialIntegrityGetsAnIndexInsteadOfAForeignKey() {
        SchemaModel model = schema(
                List.of(
                        table("Parent", primaryKey("Id"), column("Id", AccessType.AUTONUMBER_LONG)),
                        table(
                                "Child",
                                primaryKey("Id"),
                                column("Id", AccessType.AUTONUMBER_LONG),
                                column("ParentId", AccessType.LONG))),
                List.of(notEnforced("ParentChild", "Parent", "Id", "Child", "ParentId")));
        SqlitePlan plan = plan(model, profile().build());

        assertThat(plan.table("Child").orElseThrow().foreignKeys()).isEmpty();
        // Access creates no backing index for a relationship it doesn't enforce (D7)
        assertThat(indexOf(plan, "Child", "Child_ParentChild").columnNames()).containsExactly("ParentId");
        assertThat(issues.list())
                .anySatisfy(i -> assertThat(i.code()).isEqualTo(IssueCode.INDEX_ADDED_FOR_RELATIONSHIP));
    }

    @Test
    void anIndexIsNotAddedWhenAnExistingOneAlreadyCoversTheColumns() {
        SchemaModel model = schema(
                List.of(
                        table("Parent", primaryKey("Id"), column("Id", AccessType.AUTONUMBER_LONG)),
                        table(
                                "Child",
                                primaryKey("Id"),
                                List.of(index("IX_Parent", "ParentId", "Id")),
                                column("Id", AccessType.AUTONUMBER_LONG),
                                column("ParentId", AccessType.LONG))),
                List.of(notEnforced("ParentChild", "Parent", "Id", "Child", "ParentId")));
        SqlitePlan plan = plan(model, profile().build());

        assertThat(plan.table("Child").orElseThrow().indexes()).hasSize(1);
        assertThat(issues.list())
                .noneSatisfy(i -> assertThat(i.code()).isEqualTo(IssueCode.INDEX_ADDED_FOR_RELATIONSHIP));
    }

    @Test
    void hiddenColumnsAreLeftOutUnlessAskedFor() {
        SchemaModel model = schema(table(
                "T",
                primaryKey("Id"),
                column("Id", AccessType.AUTONUMBER_LONG),
                column("Name", AccessType.TEXT),
                column("s_GUID", AccessType.GUID).hidden()));
        SqlitePlan plan = plan(model, profile().build());
        assertThat(plan.table("T").orElseThrow().columns())
                .extracting(SqlitePlan.PlannedColumn::name)
                .containsExactly("Id", "Name");
        assertThat(issues.list()).anySatisfy(i -> assertThat(i.code()).isEqualTo(IssueCode.HIDDEN_COLUMN_SKIPPED));

        Issues kept = new Issues();
        SqlitePlan withHidden = SqlitePlanner.plan(
                model,
                profile().build(),
                new ConvertOptions(true, true, ConvertOptions.OnTableError.FAIL, 1000),
                SqliteOptions.DEFAULT,
                kept);
        assertThat(withHidden.table("T").orElseThrow().columns())
                .extracting(SqlitePlan.PlannedColumn::name)
                .containsExactly("Id", "Name", "s_GUID");
    }

    @Test
    void namesReservedBySqliteAndCollidingIndexNamesAreRenamed() {
        SchemaModel model = schema(
                table("sqlite_data", primaryKey("Id"), column("Id", AccessType.AUTONUMBER_LONG)),
                table("A", primaryKey("Id"), List.of(index("B_C", "Id")), column("Id", AccessType.AUTONUMBER_LONG)),
                table("A_B", primaryKey("Id"), List.of(index("C", "Id")), column("Id", AccessType.AUTONUMBER_LONG)));
        SqlitePlan plan = plan(model, profile().build());

        assertThat(plan.table("_sqlite_data")).isPresent();
        // Both indexes want the name A_B_C; the second gets a hash, and is still the same on every run
        assertThat(plan.indexes())
                .extracting(SqlitePlan.PlannedIndex::name)
                .anySatisfy(name -> assertThat(name).isEqualTo("A_B_C"))
                .anySatisfy(name -> assertThat(name).matches("A_B_C_[0-9a-f]{6}"));
        assertThat(issues.list())
                .filteredOn(i -> i.code() == IssueCode.IDENTIFIER_RENAMED)
                .hasSize(2);
    }

    @Test
    void aPrimaryKeyIsTakenFromAccessNeverFromAnAutonumber() {
        SchemaModel model = schema(
                // The autonumber is not the key: the text column is (F-32, F-35)
                table(
                        "Shippers",
                        primaryKey("Code"),
                        column("ShipperID", AccessType.AUTONUMBER_LONG),
                        column("Code", AccessType.TEXT).length(10)),
                table(
                        "Details",
                        primaryKey("OrderID", "ProductID"),
                        column("OrderID", AccessType.LONG),
                        column("ProductID", AccessType.LONG)),
                table("NoKey", null, column("Value", AccessType.LONG)));
        SqlitePlan plan = plan(
                model,
                profile()
                        .table(
                                "Shippers",
                                2,
                                stats("Code").nulls(0),
                                stats("ShipperID").nulls(0).maxAutoNumber(2))
                        .table(
                                "Details",
                                3,
                                stats("OrderID").nulls(0),
                                stats("ProductID").nulls(0))
                        .build());

        assertThat(plan.table("Shippers").orElseThrow().autoIncrementColumn()).isNull();
        assertThat(plan.table("Shippers").orElseThrow().createTableSql())
                .contains("\"ShipperID\" INTEGER NOT NULL")
                .contains("PRIMARY KEY (\"Code\")");
        assertThat(plan.table("Details").orElseThrow().createTableSql())
                .contains("PRIMARY KEY (\"OrderID\", \"ProductID\")")
                .contains("\"OrderID\" INTEGER NOT NULL")
                .contains("\"ProductID\" INTEGER NOT NULL");
        assertThat(plan.table("NoKey").orElseThrow().primaryKey()).isNull();
        assertThat(issues.list())
                .anySatisfy(i -> assertThat(i.code()).isEqualTo(IssueCode.AUTOINCREMENT_NOT_PRESERVED));
    }

    @Test
    void aSingleAscendingIntegerKeyBecomesTheRowid() {
        SchemaModel model = schema(
                table("Auto", primaryKey("Id"), column("Id", AccessType.AUTONUMBER_LONG)),
                table("Plain", primaryKey("Id"), column("Id", AccessType.LONG)),
                table("Descending", primaryKey("Id DESC"), column("Id", AccessType.LONG)),
                table("Text", primaryKey("Id"), column("Id", AccessType.TEXT).length(10)));
        SqlitePlan plan = plan(
                model,
                profile()
                        .table("Auto", 1, stats("Id").nulls(0).maxAutoNumber(9))
                        .table("Plain", 1, stats("Id").nulls(0))
                        .table("Descending", 1, stats("Id").nulls(0))
                        .table("Text", 1, stats("Id").nulls(0))
                        .build());

        assertThat(plan.table("Auto").orElseThrow().createTableSql())
                .contains("\"Id\" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL");
        assertThat(plan.table("Auto").orElseThrow().sequenceSeed()).isEqualTo(9L);
        assertThat(plan.table("Plain").orElseThrow().createTableSql()).contains("\"Id\" INTEGER PRIMARY KEY NOT NULL");
        // A descending key is not a rowid alias in SQLite, so it stays a table constraint
        assertThat(plan.table("Descending").orElseThrow().createTableSql()).contains("PRIMARY KEY (\"Id\" DESC)");
        assertThat(plan.table("Text").orElseThrow().createTableSql()).contains("PRIMARY KEY (\"Id\")");
    }

    @Test
    void strictTablesUseOnlySqlitesOwnTypes() {
        SchemaModel model = schema(table(
                "T",
                primaryKey("Id"),
                column("Id", AccessType.AUTONUMBER_LONG),
                column("Name", AccessType.TEXT).length(50),
                column("Balance", AccessType.MONEY),
                column("When", AccessType.SHORT_DATE_TIME),
                column("Flag", AccessType.BOOLEAN),
                column("Data", AccessType.OLE),
                column("Weight", AccessType.DOUBLE)));
        SqlitePlan plan = SqlitePlanner.plan(
                model,
                profile().table("T", 1, stats("Balance").digits(4, 2)).build(),
                ConvertOptions.DEFAULT,
                new SqliteOptions(true, false, false, false),
                issues);

        assertThat(plan.table("T").orElseThrow().columns())
                .extracting(SqlitePlan.PlannedColumn::declaredType)
                .containsExactly("INTEGER", "TEXT", "TEXT", "TEXT", "INTEGER", "BLOB", "REAL");
        assertThat(plan.table("T").orElseThrow().createTableSql()).endsWith(") STRICT");
    }

    @Test
    void complexColumnsKeepTheirIdAndTheirHiddenIndexBecomesTheChildTablesKey() {
        SchemaModel model = ComplexTables.expand(
                schema(table(
                        "T",
                        primaryKey("Id"),
                        List.of(unique("attach_index", "Files"), unique("history_index", "History")),
                        column("Id", AccessType.AUTONUMBER_LONG),
                        column("Files", AccessType.ATTACHMENT),
                        column("History", AccessType.VERSION_HISTORY),
                        column("Memo", AccessType.MEMO))),
                ConvertOptions.DEFAULT);
        SqlitePlan plan = plan(model, profile().build());

        assertThat(plan.table("T").orElseThrow().columns())
                .extracting(SqlitePlan.PlannedColumn::name)
                .containsExactly("Id", "Files", "Memo");
        assertThat(planned(plan, "T", "Files").declaredType()).isEqualTo("INTEGER");
        // Access's hidden unique index on the complex id is the key the child table refers to (08); v2 put it on
        // serialized attachment data (F-16)
        assertThat(indexOf(plan, "T", "T_attach_index").unique()).isTrue();
        SqlitePlan.PlannedTable child = plan.table("T_Files").orElseThrow();
        assertThat(child.columns())
                .extracting(SqlitePlan.PlannedColumn::name, SqlitePlan.PlannedColumn::declaredType)
                .containsExactly(
                        tuple("id", "INTEGER"),
                        tuple("Files_ref", "INTEGER"),
                        tuple("file_name", "VARCHAR(255)"),
                        tuple("file_type", "VARCHAR(255)"),
                        tuple("file_data", "BLOB"),
                        tuple("file_size", "BIGINT"),
                        tuple("file_url", "TEXT"),
                        tuple("file_timestamp", "DATETIME"),
                        tuple("file_flags", "INTEGER"));
        assertThat(child.primaryKey().rowidAlias()).isTrue();
        assertThat(child.foreignKeys()).singleElement().satisfies(fk -> {
            assertThat(fk.childColumns()).containsExactly("Files_ref");
            assertThat(fk.parentTable()).isEqualTo("T");
            assertThat(fk.parentColumns()).containsExactly("Files");
            assertThat(fk.onUpdate()).isEqualTo(Action.CASCADE);
            assertThat(fk.onDelete()).isEqualTo(Action.CASCADE);
        });
        assertThat(plan.table("T_History"))
                .as("version history waits for --include-version-history")
                .isEmpty();
        assertThat(issues.list())
                .noneSatisfy(i -> assertThat(i.code()).isEqualTo(IssueCode.INDEX_SKIPPED_COMPLEX_COLUMN))
                .anySatisfy(i -> assertThat(i.code()).isEqualTo(IssueCode.VERSION_HISTORY_SKIPPED));
    }

    private static SchemaModel parentAndChild() {
        return schema(
                List.of(
                        table("Parent", primaryKey("Id"), column("Id", AccessType.AUTONUMBER_LONG)),
                        table(
                                "Child",
                                primaryKey("Id"),
                                column("Id", AccessType.AUTONUMBER_LONG),
                                column("ParentId", AccessType.LONG))),
                List.of(foreignKey(
                        "ParentChild",
                        "Parent",
                        "Id",
                        "Child",
                        "ParentId",
                        "PrimaryKey",
                        Action.CASCADE,
                        Action.CASCADE)));
    }

    private SqlitePlan plan(SchemaModel model, DataProfile data) {
        return SqlitePlanner.plan(model, data, ConvertOptions.DEFAULT, SqliteOptions.DEFAULT, issues);
    }

    private static SqlitePlan.PlannedColumn planned(SqlitePlan plan, String table, String column) {
        return plan.table(table).orElseThrow().column(column).orElseThrow();
    }

    private static SqlitePlan.PlannedIndex indexOf(SqlitePlan plan, String table, String index) {
        return plan.table(table).orElseThrow().indexes().stream()
                .filter(i -> i.name().equals(index))
                .findFirst()
                .orElseThrow(() -> new AssertionError("no index " + index + " on " + table));
    }
}
