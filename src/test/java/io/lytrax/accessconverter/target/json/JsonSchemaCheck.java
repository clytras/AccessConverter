package io.lytrax.accessconverter.target.json;

import com.networknt.schema.Error;
import com.networknt.schema.Schema;
import com.networknt.schema.SchemaRegistry;
import com.networknt.schema.SpecificationVersion;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

/**
 * Validates JSON exports against the published schema, {@code accessconverter-json-v1.schema.json} (07): a document
 * whole; an ndjson directory as its {@code schema.json} plus every line of every table file against the row
 * definition {@code encoding.rows} names.
 */
public final class JsonSchemaCheck {

    private static final ObjectMapper MAPPER = JsonMapper.builder().build();
    private static final SchemaRegistry REGISTRY =
            SchemaRegistry.withDefaultDialect(SpecificationVersion.DRAFT_2020_12);
    private static final JsonNode SCHEMA_NODE = load();
    private static final Schema DOCUMENT = REGISTRY.getSchema(SCHEMA_NODE);
    private static final Schema OBJECT_ROW = row("objectRow");
    private static final Schema ARRAY_ROW = row("arrayRow");

    private JsonSchemaCheck() {}

    /** The published schema, as the jar ships it. */
    public static JsonNode schema() {
        return SCHEMA_NODE;
    }

    /** Every violation, as the validator words it; empty when the output conforms. */
    public static List<String> errors(Path output) {
        try {
            List<String> errors = new ArrayList<>();
            if (!Files.isDirectory(output)) {
                messages("", DOCUMENT.validate(MAPPER.readTree(output.toFile())), errors);
                return errors;
            }
            JsonNode schemaFile = MAPPER.readTree(
                    output.resolve(JsonFormat.NDJSON_SCHEMA_FILE).toFile());
            messages(JsonFormat.NDJSON_SCHEMA_FILE + ": ", DOCUMENT.validate(schemaFile), errors);
            Schema row = schemaFile.path("encoding").path("rows").asString("").equals("array") ? ARRAY_ROW : OBJECT_ROW;
            for (JsonNode file : schemaFile.path("files")) {
                try (BufferedReader lines =
                        Files.newBufferedReader(output.resolve(file.asString()), StandardCharsets.UTF_8)) {
                    int n = 0;
                    for (String line = lines.readLine(); line != null; line = lines.readLine()) {
                        n++;
                        messages(file.asString() + ":" + n + ": ", row.validate(MAPPER.readTree(line)), errors);
                    }
                }
            }
            return errors;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void messages(String prefix, List<Error> found, List<String> errors) {
        found.stream().limit(20).forEach(e -> errors.add(prefix + e));
    }

    private static JsonNode load() {
        try (InputStream in = JsonFormat.class.getResourceAsStream(JsonFormat.SCHEMA_RESOURCE)) {
            if (in == null) {
                throw new IllegalStateException(JsonFormat.SCHEMA_RESOURCE + " is not on the class path");
            }
            return MAPPER.readTree(in);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** One of the schema's row definitions as a schema of its own, with the definitions it refers to. */
    private static Schema row(String definition) {
        ObjectNode node = MAPPER.createObjectNode();
        node.put("$schema", SCHEMA_NODE.get("$schema").asString());
        node.put("$ref", "#/$defs/" + definition);
        node.set("$defs", SCHEMA_NODE.get("$defs"));
        return REGISTRY.getSchema(node);
    }
}
