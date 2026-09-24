package com.lytrax.accessconverter.target.json;

import com.lytrax.accessconverter.model.ForeignKeyModel;
import com.lytrax.accessconverter.model.IndexModel;
import com.lytrax.accessconverter.model.SchemaModel;
import com.lytrax.accessconverter.model.TableModel;
import com.lytrax.accessconverter.profile.DataProfile;
import com.lytrax.accessconverter.report.Issues;
import com.lytrax.accessconverter.target.ConvertOptions;
import com.lytrax.accessconverter.target.FileNames;
import com.lytrax.accessconverter.target.PlanRules;
import com.lytrax.accessconverter.target.PlanRules.WrittenColumn;
import com.lytrax.accessconverter.target.json.JsonPlan.JsonType;
import com.lytrax.accessconverter.target.json.JsonPlan.PlannedColumn;
import com.lytrax.accessconverter.target.json.JsonPlan.PlannedTable;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 * Decides what the JSON export holds (07). JSON enforces nothing, so almost nothing is downgraded: every key, index
 * and relationship is carried as Access has it, including the relationships Access doesn't enforce. What remains to
 * decide is the columns written ({@link PlanRules#writtenColumns}), each one's JSON type, and whether it is
 * {@code nullable}, which is a promise about the data and so follows the same rule as a target's NOT NULL.
 */
public final class JsonPlanner {

    private final SchemaModel model;
    private final ConvertOptions options;
    private final PlanRules rules;
    private final FileNames files = new FileNames();

    private JsonPlanner(SchemaModel model, DataProfile profile, ConvertOptions options, Issues issues) {
        this.model = model;
        this.options = options;
        this.rules = new PlanRules(profile, options, issues, "JSON");
    }

    /** @param profile null when {@code --no-profile} skipped it */
    public static JsonPlan plan(SchemaModel model, DataProfile profile, ConvertOptions options, Issues issues) {
        return new JsonPlanner(model, profile, options, issues).build();
    }

    private JsonPlan build() {
        rules.reportProfileSkipped("a column is nullable: false only where Access itself guarantees a value (Yes/No,"
                + " autonumbers and primary keys)");
        List<PlannedTable> tables = new ArrayList<>();
        List<TableModel> linked = new ArrayList<>();
        for (TableModel table : model.tables()) {
            if (table.isLinked()) {
                linked.add(table);
            } else {
                tables.add(table(table));
            }
        }
        List<ForeignKeyModel> relationships = new ArrayList<>();
        for (ForeignKeyModel fk : model.relationships()) {
            // Excluded, linked, malformed and keyless ones are already reported by the extractor
            if (fk.status() == ForeignKeyModel.Status.EMIT || fk.status() == ForeignKeyModel.Status.NOT_ENFORCED) {
                relationships.add(fk);
            }
        }
        return new JsonPlan(model, tables, relationships, linked, rules.profiled(), options);
    }

    private PlannedTable table(TableModel table) {
        Set<String> required = PlanRules.requiredColumns(table);
        Set<String> keyColumns = PlanRules.primaryKeyColumns(table);
        List<PlannedColumn> columns = new ArrayList<>();
        Set<String> written = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        for (WrittenColumn w : rules.writtenColumns(table)) {
            String name = w.column().name();
            boolean notNull = rules.notNull(table, w.column(), required.contains(name), keyColumns.contains(name));
            columns.add(new PlannedColumn(
                    w.column(), w.sourceIndex(), JsonType.of(w.column().type()), !notNull));
            written.add(name);
        }
        List<IndexModel> indexes = new ArrayList<>();
        for (IndexModel index : table.indexes()) {
            // An index on a column that isn't written (a hidden one, reported as such) describes nothing in the file
            if (written.containsAll(index.columnNames()) && !rules.skipsComplexIndex(table, index, false)) {
                indexes.add(index);
            }
        }
        return new PlannedTable(table, columns, table.primaryKey(), indexes, files.allocate(table.name(), ".ndjson"));
    }
}
