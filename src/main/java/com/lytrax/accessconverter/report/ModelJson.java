package com.lytrax.accessconverter.report;

import static com.lytrax.accessconverter.report.JsonSupport.optional;

import com.lytrax.accessconverter.model.CheckRule;
import com.lytrax.accessconverter.model.ColumnModel;
import com.lytrax.accessconverter.model.DefaultValue;
import com.lytrax.accessconverter.model.ForeignKeyModel;
import com.lytrax.accessconverter.model.IndexModel;
import com.lytrax.accessconverter.model.SchemaModel;
import com.lytrax.accessconverter.model.TableModel;
import com.lytrax.accessconverter.model.expr.ExprPrinter;
import com.lytrax.accessconverter.profile.DataProfile;
import com.lytrax.accessconverter.profile.DataProfile.ColumnStats;
import com.lytrax.accessconverter.profile.DataProfile.RelationshipProfile;
import com.lytrax.accessconverter.profile.DataProfile.RuleStats;
import com.lytrax.accessconverter.profile.DataProfile.TableProfile;
import java.io.Writer;
import java.util.List;
import java.util.Locale;
import tools.jackson.core.JsonGenerator;

/** The JSON form of {@code inspect}: the model, the optional profile and the issues. Null fields are omitted. */
public final class ModelJson {
    public static final int FORMAT_VERSION = 1;

    private ModelJson() {}

    public static void write(SchemaModel model, DataProfile profile, List<Issue> issues, Writer out) {
        try (JsonGenerator g = JsonSupport.pretty(out)) {
            g.writeStartObject();
            g.writeStringProperty("format", "accessconverter-inspect");
            g.writeNumberProperty("formatVersion", FORMAT_VERSION);
            g.writeObjectPropertyStart("source");
            g.writeStringProperty("file", model.source().fileName());
            g.writeStringProperty("fileFormat", model.source().fileFormat());
            optional(g, "codePage", model.source().codePage());
            g.writeStringProperty("charset", model.source().charset());
            g.writeEndObject();
            schema(g, model);
            if (profile != null) {
                profile(g, profile);
            }
            issues(g, issues);
            g.writeEndObject();
        }
    }

    public static void schema(JsonGenerator g, SchemaModel model) {
        g.writeObjectPropertyStart("schema");
        g.writeArrayPropertyStart("tables");
        for (TableModel t : model.tables()) {
            table(g, t);
        }
        g.writeEndArray();
        g.writeArrayPropertyStart("relationships");
        for (ForeignKeyModel fk : model.relationships()) {
            relationship(g, fk);
        }
        g.writeEndArray();
        g.writeEndObject();
    }

    private static void table(JsonGenerator g, TableModel t) {
        g.writeStartObject();
        g.writeStringProperty("name", t.name());
        if (t.isLinked()) {
            g.writeObjectPropertyStart("linked");
            g.writeStringProperty("database", t.link().displayDatabase());
            g.writeStringProperty("remoteTable", t.link().remoteTable());
            g.writeBooleanProperty("odbc", t.link().odbc());
            g.writeEndObject();
            g.writeEndObject();
            return;
        }
        g.writeNumberProperty("rowCount", t.rowCount());
        optional(g, "description", t.description());
        rule(g, "validationRule", t.validation());
        g.writeArrayPropertyStart("columns");
        for (ColumnModel c : t.columns()) {
            column(g, c);
        }
        g.writeEndArray();
        if (t.primaryKey() != null) {
            g.writeName("primaryKey");
            index(g, t.primaryKey());
        }
        g.writeArrayPropertyStart("indexes");
        for (IndexModel i : t.indexes()) {
            index(g, i);
        }
        g.writeEndArray();
        g.writeEndObject();
    }

    private static void column(JsonGenerator g, ColumnModel c) {
        g.writeStartObject();
        g.writeStringProperty("name", c.name());
        g.writeStringProperty("accessType", c.type().name());
        optional(g, "length", c.length());
        optional(g, "precision", c.precision());
        optional(g, "scale", c.scale());
        g.writeBooleanProperty("required", c.required());
        if (c.type().isText()) {
            g.writeBooleanProperty("allowZeroLength", c.allowZeroLength());
        }
        DefaultValue d = c.defaultValue();
        if (d != null) {
            g.writeObjectPropertyStart("default");
            g.writeStringProperty("access", d.raw());
            if (d.isTranslated()) {
                g.writeStringProperty("expression", ExprPrinter.print(d.expr()));
            } else {
                g.writeStringProperty("unsupported", d.unsupportedReason());
            }
            g.writeEndObject();
        }
        rule(g, "validationRule", c.validation());
        optional(g, "description", c.description());
        optional(g, "format", c.format());
        optional(g, "decimalPlaces", c.decimalPlaces());
        if (c.richText()) {
            g.writeBooleanProperty("richText", true);
        }
        optional(g, "calculatedExpression", c.calculatedExpression());
        if (c.appendOnly()) {
            g.writeBooleanProperty("appendOnly", true);
        }
        if (c.hidden()) {
            g.writeBooleanProperty("hidden", true);
        }
        g.writeEndObject();
    }

    private static void rule(JsonGenerator g, String name, CheckRule r) {
        if (r == null) {
            return;
        }
        g.writeObjectPropertyStart(name);
        g.writeStringProperty("access", r.raw());
        optional(g, "validationText", r.validationText());
        if (r.isTranslated()) {
            g.writeStringProperty("expression", ExprPrinter.print(r.expr()));
        } else {
            g.writeStringProperty("unsupported", r.unsupportedReason());
        }
        g.writeEndObject();
    }

    private static void index(JsonGenerator g, IndexModel i) {
        g.writeStartObject();
        g.writeStringProperty("name", i.name());
        g.writeArrayPropertyStart("columns");
        for (IndexModel.IndexColumn c : i.columns()) {
            g.writeStartObject();
            g.writeStringProperty("name", c.name());
            g.writeStringProperty("order", c.ascending() ? "asc" : "desc");
            g.writeEndObject();
        }
        g.writeEndArray();
        g.writeBooleanProperty("unique", i.unique());
        g.writeBooleanProperty("ignoreNulls", i.ignoreNulls());
        g.writeBooleanProperty("required", i.required());
        g.writeStringProperty("origin", lower(i.origin()));
        g.writeArrayPropertyStart("accessNames");
        i.sourceNames().forEach(g::writeString);
        g.writeEndArray();
        g.writeEndObject();
    }

    private static void relationship(JsonGenerator g, ForeignKeyModel fk) {
        g.writeStartObject();
        g.writeStringProperty("name", fk.name());
        side(g, "parent", fk.parentTable(), fk.parentColumns());
        side(g, "child", fk.childTable(), fk.childColumns());
        g.writeBooleanProperty("enforced", fk.enforced());
        g.writeStringProperty("onUpdate", lower(fk.onUpdate()));
        g.writeStringProperty("onDelete", lower(fk.onDelete()));
        g.writeBooleanProperty("oneToOne", fk.oneToOne());
        g.writeStringProperty("join", lower(fk.join()));
        g.writeNumberProperty("flags", fk.flags());
        g.writeStringProperty("status", lower(fk.status()));
        optional(g, "parentKey", fk.parentKey());
        g.writeEndObject();
    }

    private static void side(JsonGenerator g, String name, String table, List<String> columns) {
        g.writeObjectPropertyStart(name);
        g.writeStringProperty("table", table);
        g.writeArrayPropertyStart("columns");
        columns.forEach(g::writeString);
        g.writeEndArray();
        g.writeEndObject();
    }

    private static void profile(JsonGenerator g, DataProfile profile) {
        g.writeObjectPropertyStart("profile");
        g.writeArrayPropertyStart("tables");
        for (TableProfile t : profile.tables().values()) {
            g.writeStartObject();
            g.writeStringProperty("table", t.table());
            g.writeNumberProperty("rowsScanned", t.rowsScanned());
            g.writeArrayPropertyStart("columns");
            for (ColumnStats c : t.columns()) {
                g.writeStartObject();
                g.writeStringProperty("column", c.column());
                optional(g, "nulls", c.nulls());
                optional(g, "emptyStrings", c.emptyStrings());
                optional(g, "undecodable", c.undecodable());
                optional(g, "privateUse", c.privateUse());
                optional(g, "maxSignificantDigits", c.maxSignificantDigits());
                optional(g, "maxScale", c.maxScale());
                optional(g, "maxFractionDigits", c.maxFractionDigits());
                optional(g, "maxAutoNumber", c.maxAutoNumber());
                optional(g, "unsafeKeyText", c.unsafeKeyText());
                ruleStats(g, "validationRule", c.rule());
                g.writeEndObject();
            }
            g.writeEndArray();
            ruleStats(g, "tableValidationRule", t.tableRule());
            g.writeEndObject();
        }
        g.writeEndArray();
        g.writeArrayPropertyStart("relationships");
        for (RelationshipProfile r : profile.relationships().values()) {
            g.writeStartObject();
            g.writeStringProperty("relationship", r.relationship());
            g.writeNumberProperty("checked", r.checked());
            g.writeNumberProperty("orphans", r.orphans());
            strings(g, "orphanSamples", r.orphanSamples());
            g.writeNumberProperty("inexactMatches", r.inexact());
            g.writeBooleanProperty("inexactAsciiCaseOnly", r.inexactAsciiCaseOnly());
            strings(g, "inexactSamples", r.inexactSamples());
            optional(g, "scanFallback", r.scanFallback());
            g.writeEndObject();
        }
        g.writeEndArray();
        g.writeEndObject();
    }

    private static void ruleStats(JsonGenerator g, String name, RuleStats r) {
        if (r == null) {
            return;
        }
        g.writeObjectPropertyStart(name);
        g.writeNumberProperty("violations", r.violations());
        g.writeNumberProperty("unevaluable", r.unevaluable());
        optional(g, "firstError", r.firstError());
        strings(g, "samples", r.samples());
        g.writeEndObject();
    }

    public static void issues(JsonGenerator g, List<Issue> issues) {
        g.writeArrayPropertyStart("issues");
        for (Issue issue : issues) {
            g.writeStartObject();
            g.writeStringProperty("code", issue.code().name());
            g.writeStringProperty("severity", issue.severity().label());
            optional(g, "table", issue.table());
            optional(g, "object", issue.object());
            g.writeStringProperty("message", issue.message());
            g.writeNumberProperty("count", issue.count());
            strings(g, "samples", issue.samples());
            g.writeEndObject();
        }
        g.writeEndArray();
    }

    private static void strings(JsonGenerator g, String name, List<String> values) {
        g.writeArrayPropertyStart(name);
        values.forEach(g::writeString);
        g.writeEndArray();
    }

    private static String lower(Enum<?> value) {
        return value.name().toLowerCase(Locale.ROOT);
    }
}
