package com.lytrax.accessconverter.report;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

import com.lytrax.accessconverter.model.SchemaModel;
import java.io.StringWriter;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.junit.jupiter.api.Test;

class IssuesTest {

    @Test
    void repeatedIssuesAggregateWithSamples() {
        Issues issues = new Issues();
        for (int i = 0; i < 7; i++) {
            issues.add(IssueCode.CHECK_VIOLATED_BY_DATA, "T", "c", "first message wins", "row " + i);
        }
        assertThat(issues.list()).singleElement().satisfies(i -> {
            assertThat(i.count()).isEqualTo(7);
            assertThat(i.message()).isEqualTo("first message wins");
            assertThat(i.samples()).containsExactly("row 0", "row 1", "row 2", "row 3", "row 4");
        });
    }

    @Test
    void orderIsDeterministic() {
        Issues issues = new Issues();
        issues.add(IssueCode.NO_PRIMARY_KEY, "b", null, "m");
        issues.add(IssueCode.INDEX_BACKING_DROPPED, "B", ".rB", "m");
        issues.add(IssueCode.LINKED_TABLE_SKIPPED, null, null, "m");
        issues.add(IssueCode.INDEX_BACKING_DROPPED, "a", ".rB", "m");
        assertThat(issues.list())
                .extracting(Issue::table, Issue::code)
                .containsExactly(
                        tuple(null, IssueCode.LINKED_TABLE_SKIPPED),
                        tuple("a", IssueCode.INDEX_BACKING_DROPPED),
                        tuple("B", IssueCode.INDEX_BACKING_DROPPED),
                        tuple("b", IssueCode.NO_PRIMARY_KEY));
    }

    @Test
    void theHighestSeverityDecidesTheExitCode() {
        Issues issues = new Issues();
        assertThat(issues.highestSeverity()).isNull();
        issues.add(IssueCode.INDEX_BACKING_DROPPED, "T", null, "m");
        assertThat(issues.highestSeverity()).isEqualTo(Severity.INFO);
        issues.add(IssueCode.LINKED_TABLE_SKIPPED, "T", null, "m");
        assertThat(issues.highestSeverity()).isEqualTo(Severity.WARNING);
    }

    @Test
    void reportJsonIsExactAndUsesLf() {
        Issues issues = new Issues();
        issues.add(IssueCode.TABLE_EXCLUDED, "Πελάτες", null, "excluded");
        ConversionReport report = new ConversionReport(
                "3.0.0",
                "convert",
                new TreeMap<>(Map.of("to", "sqlite")),
                new SchemaModel.Source("x.accdb", "V2010"),
                List.of(new ConversionReport.TableResult("Πελάτες", 3, null)),
                issues.list(),
                "success",
                Map.of("extract", Duration.ofMillis(12)));
        StringWriter out = new StringWriter();
        report.write(out);
        assertThat(out.toString())
                .doesNotContain("\r")
                .contains("\"table\": \"Πελάτες\"")
                .contains("\"rowsRead\": 3")
                .doesNotContain("rowsWritten")
                .contains("\"extractMillis\": 12");
    }
}
