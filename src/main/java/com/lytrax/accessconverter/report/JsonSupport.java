package com.lytrax.accessconverter.report;

import java.io.Writer;
import tools.jackson.core.JsonGenerator;
import tools.jackson.core.ObjectWriteContext;
import tools.jackson.core.PrettyPrinter;
import tools.jackson.core.StreamWriteFeature;
import tools.jackson.core.json.JsonFactory;
import tools.jackson.core.util.DefaultIndenter;
import tools.jackson.core.util.DefaultPrettyPrinter;
import tools.jackson.core.util.Separators;

/**
 * JSON generators for reports: exact decimals ({@code 123456789012345.1234}, never {@code 1.23E+14}), two-space
 * indentation with LF line ends on every OS (Jackson's default indenter uses the platform separator), and the
 * target is never closed by the generator.
 */
public final class JsonSupport {
    private static final JsonFactory FACTORY = JsonFactory.builder()
            .enable(StreamWriteFeature.WRITE_BIGDECIMAL_AS_PLAIN)
            .disable(StreamWriteFeature.AUTO_CLOSE_TARGET)
            .build();

    private static final DefaultPrettyPrinter PRETTY = new DefaultPrettyPrinter()
            .withObjectIndenter(new DefaultIndenter("  ", "\n"))
            .withArrayIndenter(new DefaultIndenter("  ", "\n"))
            .withSeparators(Separators.createDefaultInstance().withObjectNameValueSpacing(Separators.Spacing.AFTER));

    private static final ObjectWriteContext PRETTY_CONTEXT = new ObjectWriteContext.Base() {
        @Override
        public PrettyPrinter getPrettyPrinter() {
            return PRETTY.createInstance();
        }

        @Override
        public boolean hasPrettyPrinter() {
            return true;
        }
    };

    private JsonSupport() {}

    public static JsonGenerator pretty(Writer out) {
        return FACTORY.createGenerator(PRETTY_CONTEXT, out);
    }

    /** Writes the property only when the value is non-null. */
    public static void optional(JsonGenerator g, String name, String value) {
        if (value != null) {
            g.writeStringProperty(name, value);
        }
    }

    public static void optional(JsonGenerator g, String name, Number value) {
        if (value != null) {
            g.writeName(name);
            switch (value) {
                case Integer i -> g.writeNumber(i);
                case Long l -> g.writeNumber(l);
                default -> g.writeNumber(value.toString());
            }
        }
    }
}
