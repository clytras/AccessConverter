package com.lytrax.accessconverter.target.json;

import java.util.Objects;

/**
 * The JSON target's options (07). None of them changes what the output holds, only how it is spelled, and the output
 * records each of them in its {@code encoding} object, so a reader (and {@code verify}) never has to be told.
 *
 * @param layout one document, or a directory of {@code schema.json} and one {@code .ndjson} file per table
 * @param rows a row as an object keyed by column name, or as an array in column order
 * @param bigint Large Number as a JSON number (exact in the file) or as a string, for parsers that lose precision
 *     past 2^53
 * @param decimals Currency and Decimal as exact JSON numbers or as strings, for parsers that read every number as a
 *     double
 * @param hyperlinks a hyperlink as Access stores it ({@code display#address#subaddress#screentip}) or split into an
 *     object
 * @param schema whether the schema section is written
 * @param stamp put the export time in {@code source}; the output then differs on every run
 */
public record JsonOptions(
        Layout layout,
        Rows rows,
        NumberForm bigint,
        NumberForm decimals,
        Hyperlinks hyperlinks,
        boolean schema,
        boolean stamp) {

    public static final JsonOptions DEFAULT = new JsonOptions(
            Layout.DOCUMENT, Rows.OBJECT, NumberForm.NUMBER, NumberForm.NUMBER, Hyperlinks.STRING, true, false);

    public enum Layout {
        DOCUMENT,
        NDJSON
    }

    public enum Rows {
        OBJECT,
        ARRAY
    }

    public enum NumberForm {
        NUMBER,
        STRING
    }

    public enum Hyperlinks {
        STRING,
        OBJECT
    }

    public JsonOptions {
        Objects.requireNonNull(layout, "layout");
        Objects.requireNonNull(rows, "rows");
        Objects.requireNonNull(bigint, "bigint");
        Objects.requireNonNull(decimals, "decimals");
        Objects.requireNonNull(hyperlinks, "hyperlinks");
    }

    public JsonOptions withLayout(Layout layout) {
        return new JsonOptions(layout, rows, bigint, decimals, hyperlinks, schema, stamp);
    }

    public JsonOptions withRows(Rows rows) {
        return new JsonOptions(layout, rows, bigint, decimals, hyperlinks, schema, stamp);
    }

    public JsonOptions withStrings() {
        return new JsonOptions(layout, rows, NumberForm.STRING, NumberForm.STRING, hyperlinks, schema, stamp);
    }

    public JsonOptions withHyperlinks(Hyperlinks hyperlinks) {
        return new JsonOptions(layout, rows, bigint, decimals, hyperlinks, schema, stamp);
    }

    public JsonOptions withoutSchema() {
        return new JsonOptions(layout, rows, bigint, decimals, hyperlinks, false, stamp);
    }
}
