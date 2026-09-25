package io.lytrax.accessconverter.target.json;

import static org.assertj.core.api.Assertions.assertThat;

import io.lytrax.accessconverter.fixtures.CorpusFile;
import io.lytrax.accessconverter.fixtures.GeneratedFixture;
import io.lytrax.accessconverter.source.OpenOptions;
import io.lytrax.accessconverter.target.BinaryMode;
import io.lytrax.accessconverter.target.ConvertOptions;
import io.lytrax.accessconverter.target.json.JsonFixture.Converted;
import io.lytrax.accessconverter.verify.VerifyResult;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.UnaryOperator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * 08 in the JSON format (version 1): OLE objects with {@code mime}, {@code {file, size}} and {@code {size}} bytes,
 * attachment, multi-value and version-history arrays. Every mode validates against the published schema and verifies;
 * the schema rejects the shapes the format doesn't allow, and {@code verify} catches an edited value in each mode.
 */
class JsonBinaryFormatTest {

    @TempDir
    Path dir;

    private Converted convert(Path source, String name, BinaryMode mode, boolean extract) {
        ConvertOptions options = ConvertOptions.DEFAULT.withBinary(mode, extract, true);
        return JsonFixture.convert(
                source, dir.resolve(name + ".json"), OpenOptions.DEFAULT, options, JsonOptions.DEFAULT);
    }

    private static Path fidelity() {
        return GeneratedFixture.SCHEMA_FIDELITY.path();
    }

    private static Path complex() {
        return CorpusFile.get("jackcess/V2010/complexDataV2010.accdb").file();
    }

    @ParameterizedTest
    @EnumSource(BinaryMode.class)
    void everyModeValidatesAndVerifies(BinaryMode mode) {
        for (boolean extract : new boolean[] {false, true}) {
            for (Path source : new Path[] {fidelity(), complex()}) {
                Converted converted = convert(source, mode + "-" + extract + "-" + source.getFileName(), mode, extract);
                assertThat(JsonSchemaCheck.errors(converted.output()))
                        .as(converted.output().toString())
                        .isEmpty();
                VerifyResult result = converted.verify();
                assertThat(result.matches()).as(result.differences().toString()).isTrue();
            }
        }
    }

    @Test
    void theOleObjectCarriesTheSniffedMime() throws IOException {
        Converted converted = convert(fidelity(), "mime", BinaryMode.INLINE, true);
        String text = Files.readString(converted.output(), StandardCharsets.UTF_8);
        assertThat(text)
                .contains("\"Img\":{\"raw\":\"iVBORw0KGgoAAAANSUhEUg==\",\"kind\":\"raw\",\"name\":null,"
                        + "\"mime\":\"image/png\",\"content\":null}")
                .contains("\"kind\":\"package\",\"name\":\"hello.txt\",\"mime\":null,\"content\":\"aGVsbG8gb2xl\"");

        // Without mime, or with a type that isn't one, the object isn't format version 1
        assertThat(JsonSchemaCheck.errors(edited(converted, "no-mime", t -> t.replace(",\"mime\":\"image/png\"", ""))))
                .isNotEmpty();
        assertThat(JsonSchemaCheck.errors(edited(
                        converted, "bad-mime", t -> t.replace("\"mime\":\"image/png\"", "\"mime\":\"PNG image\""))))
                .isNotEmpty();
        assertThat(JsonSchemaCheck.errors(
                        edited(converted, "bad-kind", t -> t.replace("\"kind\":\"raw\"", "\"kind\":\"picture\""))))
                .isNotEmpty();
        // A mime that doesn't match the bytes is a difference, as is a wrong kind
        assertThat(converted
                        .verify(edited(
                                converted,
                                "other-mime",
                                t -> t.replace("\"mime\":\"image/png\"", "\"mime\":\"image/gif\"")))
                        .matches())
                .isFalse();
    }

    @Test
    void omitWritesTheByteCountAndOnlyIt() throws IOException {
        Converted converted = convert(fidelity(), "omit", BinaryMode.OMIT, false);
        String text = Files.readString(converted.output(), StandardCharsets.UTF_8);
        // NULL stays null, an empty value is 0, a value its size
        assertThat(text)
                .contains("{\"FileID\":1,\"Raw\":{\"size\":16},\"Doc\":{\"size\":255},\"Img\":{\"size\":16}}")
                .contains("{\"FileID\":2,\"Raw\":null,\"Doc\":null,\"Img\":null}")
                .contains("{\"FileID\":3,\"Raw\":{\"size\":0},\"Doc\":{\"size\":0},\"Img\":null}");
        assertThat(text).contains("\"binary\": \"omit\"");

        assertThat(JsonSchemaCheck.errors(edited(
                        converted,
                        "omit-extra",
                        t -> t.replace("\"Raw\":{\"size\":16}", "\"Raw\":{\"size\":16,\"x\":1}"))))
                .isNotEmpty();
        assertThat(JsonSchemaCheck.errors(edited(
                        converted, "omit-negative", t -> t.replace("\"Raw\":{\"size\":16}", "\"Raw\":{\"size\":-1}"))))
                .isNotEmpty();
        // verify compares sizes in this mode
        VerifyResult result = converted.verify(
                edited(converted, "omit-size", t -> t.replace("\"Raw\":{\"size\":16}", "\"Raw\":{\"size\":15}")));
        assertThat(result.differences()).singleElement().satisfies(d -> {
            assertThat(d.table()).isEqualTo("Files");
            assertThat(d.object()).isEqualTo("Raw");
            assertThat(d.expected()).isEqualTo("16");
            assertThat(d.actual()).isEqualTo("15");
        });
        // A size where there was no value, or base64 where the file says omit, is a difference too
        assertThat(converted
                        .verify(edited(
                                converted,
                                "omit-null",
                                t -> t.replace("{\"FileID\":2,\"Raw\":null", "{\"FileID\":2,\"Raw\":{\"size\":0}")))
                        .matches())
                .isFalse();
    }

    @Test
    void filesModeIsVerifiedAgainstTheFilesThemselves() throws IOException {
        Converted converted = convert(fidelity(), "files", BinaryMode.FILES, true);
        String text = Files.readString(converted.output(), StandardCharsets.UTF_8);
        assertThat(text)
                .contains(
                        "\"Doc\":{\"raw\":{\"file\":\"files.json-files/Files/Doc/1.bin\",\"size\":255},\"kind\":\"package\","
                                + "\"name\":\"hello.txt\",\"mime\":null,\"content\":{\"file\":"
                                + "\"files.json-files/Files/Doc__content/1-hello.txt\",\"size\":9}}");
        Path content = dir.resolve("files.json-files/Files/Doc__content/1-hello.txt");
        assertThat(content).hasContent("hello ole");
        assertThat(converted.verify().matches()).isTrue();

        Files.writeString(content, "hello OLE");
        assertThat(converted.verify().matches()).as("an edited file").isFalse();
        Files.delete(content);
        assertThat(converted.verify().matches()).as("a missing file").isFalse();

        assertThat(JsonSchemaCheck.errors(
                        edited(converted, "files-nosize", t -> t.replaceFirst(",\"size\":255}", "}"))))
                .isNotEmpty();
    }

    @Test
    void complexValuesAreArraysTheSchemaKnows() throws IOException {
        Converted converted = convert(complex(), "complex", BinaryMode.INLINE, false);
        String text = Files.readString(converted.output(), StandardCharsets.UTF_8);
        assertThat(text)
                .contains("\"type\": \"attachments\"", "\"type\": \"multiValue\"", "\"type\": \"versionHistory\"")
                .contains("\"multi-value-data\":[\"value1\",\"value2\",\"value3\",\"value4\"]")
                .contains("{\"value\":\"row3-memo-revised\",\"modified\":\"2011-09-12T21:22:33.077\"}");

        assertThat(JsonSchemaCheck.errors(edited(
                        converted, "attachment-both", t -> t.replaceFirst("\"data\":\"", "\"file\":null,\"data\":\""))))
                .as("data and file together")
                .isNotEmpty();
        assertThat(JsonSchemaCheck.errors(edited(
                        converted,
                        "multivalue-without-element",
                        t -> t.replaceFirst(
                                "\"element\": \\{\\s*\"type\": \"string\",\\s*\"accessType\": \"TEXT\",\\s*\"length\": 255\\s*}",
                                "\"description\": \"x\""))))
                .as("a multiValue column without its element")
                .isNotEmpty();
        assertThat(converted
                        .verify(edited(converted, "multivalue-edit", t -> t.replace("\"value3\"", "\"value5\"")))
                        .differences())
                .singleElement()
                .satisfies(d -> assertThat(d.object()).isEqualTo("multi-value-data"));
        assertThat(converted
                        .verify(edited(converted, "attachment-edit", t -> t.replace("\"size\":38", "\"size\":39")))
                        .matches())
                .isFalse();
    }

    private Path edited(Converted converted, String name, UnaryOperator<String> edit) throws IOException {
        String original = Files.readString(converted.output(), StandardCharsets.UTF_8);
        String changed = edit.apply(original);
        assertThat(changed).as("the edit applies").isNotEqualTo(original);
        Path copy = converted.output().resolveSibling(name + "-edited.json");
        Files.writeString(copy, changed, StandardCharsets.UTF_8);
        return copy;
    }
}
