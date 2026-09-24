package com.lytrax.accessconverter.target.json;

import tools.jackson.core.JsonGenerator;
import tools.jackson.core.ObjectReadContext;
import tools.jackson.core.ObjectWriteContext;
import tools.jackson.core.PrettyPrinter;
import tools.jackson.core.StreamReadFeature;
import tools.jackson.core.StreamWriteFeature;
import tools.jackson.core.json.JsonFactory;
import tools.jackson.core.json.JsonWriteFeature;

/**
 * The constants of the JSON format (07) and the Jackson set-up both the writer and the verifier use. The format is a
 * contract, published as {@value #SCHEMA_RESOURCE}: {@link #VERSION} changes with any change a reader could trip
 * over.
 */
public final class JsonFormat {

    public static final String NAME = "accessconverter";

    public static final int VERSION = 1;

    /** The published JSON Schema, next to this class. */
    public static final String SCHEMA_RESOURCE = "accessconverter-json-v1.schema.json";

    /** The ndjson layout's description of the export, beside the table files. */
    public static final String NDJSON_SCHEMA_FILE = "schema.json";

    public static final String NDJSON_EXTENSION = ".ndjson";

    /**
     * Exact decimals ({@code 123456789012345.1234}, never {@code 1.23E+14}); URLs without {@code \/}; emoji as UTF-8
     * rather than as escaped surrogate pairs; the stream never closed by the generator, and nothing closed for us on
     * the way out, so a failed table can be cut off cleanly; no separator between root values (each ndjson row ends
     * with its own line feed).
     */
    static final JsonFactory FACTORY = JsonFactory.builder()
            .enable(StreamWriteFeature.WRITE_BIGDECIMAL_AS_PLAIN)
            .disable(StreamWriteFeature.AUTO_CLOSE_TARGET)
            .disable(StreamWriteFeature.AUTO_CLOSE_CONTENT)
            .disable(JsonWriteFeature.ESCAPE_FORWARD_SLASHES)
            .enable(JsonWriteFeature.COMBINE_UNICODE_SURROGATES_IN_UTF8)
            .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
            .rootValueSeparator((String) null)
            .build();

    private JsonFormat() {}

    /** A generator for the document and {@code schema.json}: two-space indentation, LF line ends on every OS. */
    static ObjectWriteContext pretty(Printer printer) {
        return new ObjectWriteContext.Base() {
            @Override
            public PrettyPrinter getPrettyPrinter() {
                return printer;
            }

            @Override
            public boolean hasPrettyPrinter() {
                return true;
            }
        };
    }

    static ObjectReadContext reading() {
        return ObjectReadContext.empty();
    }

    /**
     * Indents like {@code JsonSupport}'s reports, with one difference: a data table's rows are written by a second,
     * compact generator (one row per line), so the array they sit in must be closed on a new line although this
     * generator saw no values in it.
     */
    static final class Printer implements PrettyPrinter {
        private int nesting;
        private boolean valuesWrittenElsewhere;

        /** The array about to be closed holds rows another generator wrote. */
        void valuesWrittenElsewhere() {
            valuesWrittenElsewhere = true;
        }

        /** The indentation of a value inside the array that is open now, for the rows the other generator writes. */
        String rowIndent() {
            return "\n" + "  ".repeat(nesting);
        }

        private void newLine(JsonGenerator g) {
            g.writeRaw('\n');
            for (int i = 0; i < nesting; i++) {
                g.writeRaw("  ");
            }
        }

        @Override
        public void writeRootValueSeparator(JsonGenerator g) {}

        @Override
        public void writeStartObject(JsonGenerator g) {
            g.writeRaw('{');
            nesting++;
        }

        @Override
        public void beforeObjectEntries(JsonGenerator g) {
            newLine(g);
        }

        @Override
        public void writeObjectNameValueSeparator(JsonGenerator g) {
            g.writeRaw(": ");
        }

        @Override
        public void writeObjectEntrySeparator(JsonGenerator g) {
            g.writeRaw(',');
            newLine(g);
        }

        @Override
        public void writeEndObject(JsonGenerator g, int entries) {
            nesting--;
            if (entries > 0) {
                newLine(g);
            }
            g.writeRaw('}');
        }

        @Override
        public void writeStartArray(JsonGenerator g) {
            g.writeRaw('[');
            nesting++;
        }

        @Override
        public void beforeArrayValues(JsonGenerator g) {
            newLine(g);
        }

        @Override
        public void writeArrayValueSeparator(JsonGenerator g) {
            g.writeRaw(',');
            newLine(g);
        }

        @Override
        public void writeEndArray(JsonGenerator g, int values) {
            nesting--;
            if (values > 0 || valuesWrittenElsewhere) {
                newLine(g);
            }
            valuesWrittenElsewhere = false;
            g.writeRaw(']');
        }
    }
}
