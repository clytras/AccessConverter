package io.lytrax.accessconverter.cli;

import io.lytrax.accessconverter.model.CheckRule;
import io.lytrax.accessconverter.model.ColumnModel;
import io.lytrax.accessconverter.model.DefaultValue;
import io.lytrax.accessconverter.model.ForeignKeyModel;
import io.lytrax.accessconverter.model.ForeignKeyModel.Action;
import io.lytrax.accessconverter.model.IndexModel;
import io.lytrax.accessconverter.model.SchemaModel;
import io.lytrax.accessconverter.model.TableModel;
import io.lytrax.accessconverter.model.expr.ExprPrinter;
import io.lytrax.accessconverter.profile.DataProfile;
import io.lytrax.accessconverter.profile.DataProfile.ColumnStats;
import io.lytrax.accessconverter.profile.DataProfile.RelationshipProfile;
import io.lytrax.accessconverter.profile.DataProfile.RuleStats;
import io.lytrax.accessconverter.profile.DataProfile.TableProfile;
import io.lytrax.accessconverter.report.Issue;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

/**
 * The text form of {@code inspect}: the normalized model, optionally the profile, then the issues. Deterministic,
 * with LF line ends on every OS, so it can be compared byte for byte (golden tests).
 */
final class ModelText {
    private final StringBuilder out = new StringBuilder();

    static String render(SchemaModel model, DataProfile profile, List<Issue> issues) {
        ModelText text = new ModelText();
        text.model(model);
        if (profile != null) {
            text.profile(profile);
        }
        text.issues(issues);
        return text.out.toString();
    }

    private void model(SchemaModel model) {
        long local = model.tables().stream()
                .filter(t -> !t.isLinked() && !t.isResolvedLink())
                .count();
        long resolved =
                model.tables().stream().filter(TableModel::isResolvedLink).count();
        SchemaModel.Source source = model.source();
        line("Source: " + source.fileName() + " (" + source.fileFormat() + ")");
        line("Text: " + (source.codePage() == null ? "" : "code page " + source.codePage() + ", ") + "charset "
                + source.charset());
        line("Tables: " + local + " local, " + (model.tables().size() - local) + " linked"
                + (resolved == 0 ? "" : " (" + resolved + " read from their back-end)") + "; relationships: "
                + model.relationships().size());
        for (TableModel table : model.tables()) {
            line("");
            table(table);
        }
        if (!model.relationships().isEmpty()) {
            line("");
            line("Relationships:");
            model.relationships().forEach(this::relationship);
        }
    }

    private void table(TableModel table) {
        if (table.isLinked()) {
            TableModel.LinkInfo link = table.link();
            line("Linked table " + table.name() + " -> " + link.remoteTable() + " in " + link.displayDatabase()
                    + (link.odbc() ? " (ODBC)" : ""));
            return;
        }
        line("Table " + table.name() + " (" + table.rowCount() + " rows)");
        if (table.isResolvedLink()) {
            TableModel.LinkInfo link = table.link();
            line("  linked to " + link.remoteTable() + " in " + link.displayDatabase() + ", read from "
                    + link.readFrom());
        }
        if (table.description() != null) {
            line("  description: " + quote(table.description()));
        }
        if (table.validation() != null) {
            line("  validation: " + rule(table.validation()));
        }
        line("  columns:");
        int width =
                table.columns().stream().mapToInt(c -> c.name().length()).max().orElse(0);
        for (ColumnModel column : table.columns()) {
            line("    " + pad(column.name(), width) + "  " + column(column));
        }
        if (table.primaryKey() != null) {
            line("  primary key: " + index(table.primaryKey()));
        } else {
            line("  primary key: none");
        }
        if (!table.indexes().isEmpty()) {
            line("  indexes:");
            table.indexes().forEach(i -> line("    " + index(i)));
        }
    }

    private static String column(ColumnModel c) {
        List<String> parts = new ArrayList<>();
        String type = c.type().name();
        if (c.length() != null) {
            type += "(" + c.length() + ")";
        } else if (c.precision() != null) {
            type += "(" + c.precision() + "," + c.scale() + ")";
        }
        parts.add(type);
        if (c.required()) {
            parts.add("required");
        }
        if (c.type().isText() && !c.allowZeroLength()) {
            parts.add("no-empty-string");
        }
        if (c.hidden()) {
            parts.add("hidden");
        }
        if (c.appendOnly()) {
            parts.add("append-only");
        }
        if (c.richText()) {
            parts.add("rich-text");
        }
        if (c.isCalculated()) {
            parts.add("calculated " + quote(c.calculatedExpression()));
        }
        if (c.defaultValue() != null) {
            parts.add("default " + value(c.defaultValue()));
        }
        if (c.validation() != null) {
            parts.add("check " + rule(c.validation()));
        }
        if (c.format() != null) {
            parts.add("format " + quote(c.format()));
        }
        if (c.decimalPlaces() != null) {
            parts.add("decimal-places " + (c.decimalPlaces() == 255 ? "auto" : c.decimalPlaces()));
        }
        if (c.description() != null) {
            parts.add("description " + quote(c.description()));
        }
        return String.join("  ", parts);
    }

    /** The normalized expression, plus the Access text when it differs (e.g. {@code 0 from "=0"}). */
    private static String value(DefaultValue d) {
        return d.isTranslated()
                ? withSource(ExprPrinter.print(d.expr()), d.raw())
                : quote(d.raw()) + " (untranslated: " + d.unsupportedReason() + ")";
    }

    private static String rule(CheckRule r) {
        String text = r.isTranslated()
                ? withSource(ExprPrinter.print(r.expr()), r.raw())
                : quote(r.raw()) + " (untranslated: " + r.unsupportedReason() + ")";
        return r.validationText() == null ? text : text + " message " + quote(r.validationText());
    }

    private static String withSource(String normalized, String raw) {
        return normalized.equals(raw.strip()) ? normalized : normalized + " from " + quote(raw);
    }

    private static String index(IndexModel i) {
        List<String> flags = new ArrayList<>();
        if (i.unique() && !i.primaryKey()) {
            flags.add("unique");
        }
        if (i.ignoreNulls()) {
            flags.add("ignore-nulls");
        }
        if (i.required() && !i.primaryKey()) {
            flags.add("required");
        }
        if (i.origin() == IndexModel.Origin.RELATIONSHIP) {
            flags.add("relationship");
        }
        String columns = i.columns().stream()
                .map(c -> c.name() + (c.ascending() ? "" : " DESC"))
                .collect(Collectors.joining(", ", "(", ")"));
        String text = i.name() + " " + columns + (flags.isEmpty() ? "" : " " + String.join(" ", flags));
        if (i.sourceNames().size() > 1) {
            text += " [merged: "
                    + String.join(
                            ", ", i.sourceNames().subList(1, i.sourceNames().size())) + "]";
        }
        return text;
    }

    private void relationship(ForeignKeyModel fk) {
        List<String> parts = new ArrayList<>();
        parts.add(fk.enforced() ? "enforced" : "not enforced");
        if (fk.oneToOne()) {
            parts.add("one-to-one");
        }
        if (fk.onUpdate() != Action.NO_ACTION) {
            parts.add("on update " + action(fk.onUpdate()));
        }
        if (fk.onDelete() != Action.NO_ACTION) {
            parts.add("on delete " + action(fk.onDelete()));
        }
        if (fk.join() != ForeignKeyModel.Join.INNER) {
            parts.add(fk.join().name().toLowerCase(Locale.ROOT).replace('_', ' ') + " join");
        }
        String status = fk.status().name() + (fk.parentKey() != null ? " via " + fk.parentKey() : "");
        line("  " + fk.name() + ": " + fk.parentTable() + " " + list(fk.parentColumns()) + " -> " + fk.childTable()
                + " " + list(fk.childColumns()) + "; " + String.join(", ", parts) + " [" + status + "]");
    }

    private void profile(DataProfile profile) {
        line("");
        line("Profile:");
        for (TableProfile table : profile.tables().values()) {
            List<String> lines = new ArrayList<>();
            for (ColumnStats c : table.columns()) {
                List<String> parts = new ArrayList<>();
                if (c.nulls() != null) {
                    parts.add("nulls " + c.nulls());
                }
                if (c.emptyStrings() != null) {
                    parts.add("empty-strings " + c.emptyStrings());
                }
                if (c.undecodable() != null) {
                    parts.add("undecodable " + c.undecodable() + " (bytes the code page doesn't define)");
                }
                if (c.privateUse() != null) {
                    parts.add("private-use " + c.privateUse() + " (bytes Windows decodes to its private use area)");
                }
                if (c.maxSignificantDigits() != null) {
                    parts.add("max-digits " + c.maxSignificantDigits() + " max-scale " + c.maxScale());
                }
                if (c.maxFractionDigits() != null) {
                    parts.add("fraction-digits " + c.maxFractionDigits());
                }
                if (c.maxAutoNumber() != null) {
                    parts.add("max-autonumber " + c.maxAutoNumber());
                }
                if (c.unsafeKeyText() != null) {
                    parts.add("unsafe-key-text " + c.unsafeKeyText() + " (characters the default MySQL collations"
                            + " compare unlike Access)");
                }
                if (c.rule() != null) {
                    parts.add("check " + ruleStats(c.rule()));
                }
                lines.add("    " + c.column() + ": " + String.join(", ", parts));
            }
            if (table.tableRule() != null) {
                lines.add("    table rule: " + ruleStats(table.tableRule()));
            }
            line("  " + table.table() + ": " + table.rowsScanned() + " rows scanned");
            lines.forEach(this::line);
        }
        for (RelationshipProfile r : profile.relationships().values()) {
            String text = "  " + r.relationship() + ": " + r.checked() + " keys checked, " + r.orphans() + " orphans";
            if (r.inexact() > 0) {
                text += ", " + r.inexact() + " inexact matches"
                        + (r.inexactAsciiCaseOnly() ? " (ASCII case only)" : "");
            }
            if (r.scanFallback() != null) {
                text += "; parent keys scanned instead of the index (" + r.scanFallback() + ")";
            }
            line(text + samples(r.orphanSamples()) + samples(r.inexactSamples()));
        }
    }

    private static String ruleStats(RuleStats r) {
        String text = r.violations() + " violations";
        if (r.unevaluable() > 0) {
            text += ", " + r.unevaluable() + " unevaluable (" + r.firstError() + ")";
        }
        return text + samples(r.samples());
    }

    private void issues(List<Issue> issues) {
        line("");
        line("Issues: " + issues.size());
        for (Issue issue : issues) {
            String where = issue.table() == null
                    ? ""
                    : " " + issue.table() + (issue.object() == null ? "" : " / " + issue.object());
            line("  " + issue.severity().label() + " " + issue.code() + where + ": " + issue.message()
                    + (issue.count() > 1 ? " (x" + issue.count() + ")" : "") + samples(issue.samples()));
        }
    }

    private static String samples(List<String> samples) {
        return samples.isEmpty() ? "" : " e.g. " + String.join("; ", samples);
    }

    private static String action(Action action) {
        return action == Action.CASCADE ? "cascade" : "set null";
    }

    private static String list(List<String> names) {
        return "(" + String.join(", ", names) + ")";
    }

    private static String quote(String s) {
        return "\""
                + s.replace("\\", "\\\\")
                        .replace("\"", "\\\"")
                        .replace("\n", "\\n")
                        .replace("\r", "\\r") + "\"";
    }

    private static String pad(String s, int width) {
        return s + " ".repeat(Math.max(0, width - s.length()));
    }

    private void line(String text) {
        out.append(text).append('\n');
    }
}
