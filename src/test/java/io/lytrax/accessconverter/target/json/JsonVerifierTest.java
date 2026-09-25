package io.lytrax.accessconverter.target.json;

import static org.assertj.core.api.Assertions.assertThat;

import io.lytrax.accessconverter.fixtures.CorpusFile;
import io.lytrax.accessconverter.fixtures.GeneratedFixture;
import io.lytrax.accessconverter.target.json.JsonFixture.Converted;
import io.lytrax.accessconverter.target.json.JsonOptions.Layout;
import io.lytrax.accessconverter.verify.VerifyResult;
import io.lytrax.accessconverter.verify.VerifyResult.Difference;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.UnaryOperator;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * {@code verify} and the published schema catch what they are there to catch: an export that verifies after an edit,
 * or validates while it breaks the format, would make the corpus test prove nothing.
 */
class JsonVerifierTest {

    @TempDir
    static Path dir;

    static Converted fidelity;
    static Converted extended;

    @BeforeAll
    static void convert() {
        fidelity = JsonFixture.convert(GeneratedFixture.SCHEMA_FIDELITY.path(), dir.resolve("fidelity.json"));
        extended = JsonFixture.convert(
                CorpusFile.get("jackcess/V2019/extDateV2019.accdb").file(), dir.resolve("extended.json"));
    }

    @Test
    void theUneditedExportsMatch() {
        assertThat(fidelity.verify().matches()).isTrue();
        assertThat(extended.verify().matches()).isTrue();
    }

    @Test
    void anEditedValueIsADifference() throws IOException {
        VerifyResult result =
                fidelity.verify(edited(fidelity, "fidelity-value", t -> t.replace("\"Rating\":255", "\"Rating\":254")));

        assertThat(result.differences()).singleElement().satisfies(d -> {
            assertThat(d.table()).isEqualTo("Customers");
            assertThat(d.object()).isEqualTo("Rating");
            assertThat(d.expected()).isEqualTo("255");
            assertThat(d.actual()).isEqualTo("254");
        });
    }

    @Test
    void aDecimalMustStayExactAndPlain() throws IOException {
        // The same value in exponent form is not what 07 promises
        VerifyResult result = fidelity.verify(
                edited(fidelity, "fidelity-exponent", t -> t.replace("\"UnitPrice\":9.9900", "\"UnitPrice\":9.99E0")));

        assertThat(result.differences())
                .singleElement()
                .satisfies(d -> assertThat(d.actual()).contains("not a plain decimal"));
    }

    @Test
    void anExtendedDateWithoutItsSevenDigitsIsADifference() throws IOException {
        VerifyResult result = extended.verify(edited(
                extended,
                "extended-digits",
                t -> t.replaceFirst(
                        "\"DateExt\":\"2020-06-17T00:00:00.0000000\"", "\"DateExt\":\"2020-06-17T00:00:00\"")));

        assertThat(result.differences())
                .singleElement()
                .satisfies(d -> assertThat(d.object()).isEqualTo("DateExt"));
    }

    @Test
    void aMissingRowIsADifference() throws IOException {
        VerifyResult result = fidelity.verify(edited(
                fidelity,
                "fidelity-row",
                t -> t.replace("      {\"ShipperID\":2,\"Code\":\"UPS\",\"Name\":\"United Parcel\"}\n", "")
                        .replace(
                                "{\"ShipperID\":1,\"Code\":\"DHL\",\"Name\":\"DHL Express\"},",
                                "{\"ShipperID\":1," + "\"Code\":\"DHL\",\"Name\":\"DHL Express\"}")));

        assertThat(result.differences())
                .extracting(Difference::table, Difference::expected, Difference::actual)
                .containsExactly(org.assertj.core.groups.Tuple.tuple("Shippers", "a row", "no row"));
    }

    @Test
    void anEditedSchemaIsADifferenceNamedByItsPath() throws IOException {
        VerifyResult result = fidelity.verify(edited(
                fidelity,
                "fidelity-schema",
                t -> t.replaceFirst("\"onDelete\": \"cascade\"", "\"onDelete\": \"noAction\"")));

        assertThat(result.differences()).singleElement().satisfies(d -> {
            assertThat(d.what()).matches("schema\\.relationships\\[\\d+]\\.onDelete");
            assertThat(d.expected()).isEqualTo("cascade");
        });
    }

    @Test
    void aTruncatedDocumentIsNotValidJson() throws IOException {
        VerifyResult result = fidelity.verify(edited(fidelity, "fidelity-cut", t -> t.substring(0, t.length() / 2)));

        assertThat(result.matches()).isFalse();
        assertThat(result.differences().getLast().actual()).startsWith("not valid JSON");
    }

    @Test
    void anNdjsonDirectoryMustHoldEveryTableFileAndNothingElse() throws IOException {
        Converted ndjson = JsonFixture.convert(
                GeneratedFixture.SCHEMA_FIDELITY.path(),
                dir.resolve("fidelity-ndjson"),
                JsonOptions.DEFAULT.withLayout(Layout.NDJSON));
        assertThat(ndjson.verify().matches()).isTrue();

        Files.delete(ndjson.output().resolve("Shippers.ndjson"));
        Files.writeString(ndjson.output().resolve("stray.txt"), "x");
        VerifyResult result = ndjson.verify();

        assertThat(result.differences())
                .extracting(Difference::what)
                .contains("the directory", "the file Shippers.ndjson");
    }

    @Test
    void theSchemaRejectsWhatTheFormatDoesNotAllow() throws IOException {
        assertThat(JsonSchemaCheck.errors(fidelity.output())).isEmpty();

        assertThat(JsonSchemaCheck.errors(edited(
                        fidelity, "invalid-type", t -> t.replaceFirst("\"type\": \"int32\"", "\"type\": \"int\""))))
                .isNotEmpty();
        assertThat(JsonSchemaCheck.errors(edited(
                        fidelity,
                        "invalid-property",
                        t -> t.replaceFirst("\"layout\": \"document\"", "\"layout\": \"document\",\n  \"extra\": 1"))))
                .isNotEmpty();
        assertThat(JsonSchemaCheck.errors(edited(fidelity, "invalid-row", t -> t.replace("{\"TierID\":1}", "[1]"))))
                .as("an array row where encoding.rows says object")
                .isNotEmpty();
        assertThat(JsonSchemaCheck.errors(edited(
                        fidelity,
                        "invalid-version",
                        t -> t.replaceFirst("\"formatVersion\": 1", "\"formatVersion\": 2"))))
                .isNotEmpty();
    }

    private static Path edited(Converted converted, String name, UnaryOperator<String> edit) throws IOException {
        String original = Files.readString(converted.output(), StandardCharsets.UTF_8);
        String changed = edit.apply(original);
        assertThat(changed).as("the edit applies").isNotEqualTo(original);
        Path copy = dir.resolve(name + ".json");
        Files.writeString(copy, changed, StandardCharsets.UTF_8);
        return copy;
    }
}
