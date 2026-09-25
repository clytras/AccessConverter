package io.lytrax.accessconverter.cli;

import static org.assertj.core.api.Assertions.assertThat;

import io.lytrax.accessconverter.model.AccessType;
import io.lytrax.accessconverter.model.TableModel;
import io.lytrax.accessconverter.source.ReadListener;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/** The progress line: when it is drawn, what it says, and that it stays cheap when standard error is a log. */
class ProgressTest {

    private final StringWriter err = new StringWriter();
    private long now;
    private final Progress progress = Progress.on(new PrintWriter(err), () -> now);

    private static TableModel table(String name, long rows) {
        return new TableModel(name, null, List.of(), null, List.of(), null, null, rows);
    }

    private void advance(long millis) {
        now += TimeUnit.MILLISECONDS.toNanos(millis);
    }

    /** The lines drawn, in order, each without its padding. */
    private List<String> drawn() {
        return Arrays.stream(err.toString().split("\r"))
                .map(String::stripTrailing)
                .filter(s -> !s.isEmpty())
                .toList();
    }

    @Test
    void nothingIsDrawnInTheFirstHalfSecond() {
        TableModel orders = table("Orders", 1_000);
        progress.stage("write", List.of(orders));
        advance(499);
        Runnable row = progress.started(orders);
        for (int i = 0; i < 1_000; i++) {
            row.run();
        }
        progress.close();
        assertThat(err.toString()).isEmpty();
    }

    @Test
    void aTableStartIsDrawnWithItsStageAndPosition() {
        TableModel customers = table("Customers", 20);
        TableModel orders = table("Orders", 120_000);
        TableModel linked = new TableModel(
                "Linked",
                new TableModel.LinkInfo("C:\\data\\backend.mdb", "Linked", false),
                List.of(),
                null,
                List.of(),
                null,
                null,
                0);
        progress.stage("write", List.of(customers, orders, linked));
        advance(600);
        progress.started(orders);
        // A linked table has no rows to read, so it doesn't count
        assertThat(drawn()).containsExactly("write 2/2 Orders  0 / 120,000 rows  0%");
    }

    @Test
    void aTableRedrawsOnlyWhenItsPercentageChangesAndAtMostFiveTimesASecond() {
        TableModel orders = table("Orders", 100_000);
        progress.stage("profile", List.of(orders));
        advance(600);
        Runnable row = progress.started(orders);
        // A clock that moves 10 ms a row: time never holds a redraw back, the percentage does
        for (int i = 0; i < 100_000; i++) {
            advance(10);
            row.run();
        }
        List<String> lines = drawn();
        assertThat(lines).hasSizeLessThanOrEqualTo(101).hasSizeGreaterThan(90);
        assertThat(lines.get(lines.size() - 1)).isEqualTo("profile 1/1 Orders  100,000 / 100,000 rows  100%");

        // A clock that stands still: the table start is the only redraw, however many rows follow
        StringWriter still = new StringWriter();
        long[] frozen = {0};
        Progress stopped = Progress.on(new PrintWriter(still), () -> frozen[0]);
        frozen[0] += TimeUnit.SECONDS.toNanos(1);
        Runnable counted = stopped.started(orders);
        for (int i = 0; i < 100_000; i++) {
            counted.run();
        }
        assertThat(still.toString().chars().filter(c -> c == '\r')).hasSize(1);
    }

    @Test
    void aTableWithoutATotalShowsItsCount() {
        TableModel child = new TableModel(
                "Orders_Files",
                null,
                List.of(),
                null,
                List.of(),
                null,
                null,
                0,
                new TableModel.ComplexSource("Orders", "Files", AccessType.ATTACHMENT, null));
        progress.stage("write", List.of(child));
        advance(600);
        Runnable row = progress.started(child);
        advance(300);
        for (int i = 0; i < 64; i++) {
            row.run();
        }
        assertThat(drawn()).containsExactly("write 1/1 Orders_Files  0 rows", "write 1/1 Orders_Files  64 rows");
        assertThat(progress.line(child1())).isEqualTo("write 1/1 Orders_Files  1 row");
    }

    private Progress.Pass child1() {
        Progress.Pass pass = progress.new Pass("Orders_Files", -1);
        pass.rows = 1;
        return pass;
    }

    @Test
    void theLineIsErasedAtTheEnd() {
        TableModel orders = table("Orders", 10);
        progress.stage("verify", List.of(orders));
        advance(600);
        progress.started(orders);
        progress.close();
        String line = "verify 1/1 Orders  0 / 10 rows  0%";
        assertThat(err.toString()).isEqualTo("\r" + line + "\r" + " ".repeat(line.length()) + "\r");
    }

    @Test
    void aShorterLineCoversTheLongerOneBeforeIt() {
        TableModel longer = table("OrderDetails", 1_000_000);
        TableModel shorter = table("X", 1);
        progress.stage("write", List.of(longer, shorter));
        advance(600);
        progress.started(longer);
        advance(300);
        progress.started(shorter);
        String first = "write 1/2 OrderDetails  0 / 1,000,000 rows  0%";
        String second = "write 2/2 X  0 / 1 rows  0%";
        assertThat(err.toString())
                .isEqualTo("\r" + first + "\r" + second + " ".repeat(first.length() - second.length()));
    }

    @Test
    void aLongTableNameIsShortenedToFitEightyColumns() {
        TableModel table = table("A".repeat(120), 5_000_000);
        progress.stage("write", List.of(table));
        advance(600);
        progress.started(table);
        String line = drawn().get(0);
        assertThat(line).hasSize(Progress.WIDTH).endsWith("A...  0 / 5,000,000 rows  0%");
        // Never half an emoji: when the cut falls inside a surrogate pair, the whole character goes
        String emoji = Character.toString(0x1F600);
        assertThat(Progress.fit("a" + emoji + "bcdef", 5)).isEqualTo("a...");
        assertThat(Progress.fit("ab" + emoji + "cdef", 6)).isEqualTo("ab...");
    }

    @Test
    void aNestedPassHandsTheLineBackWhenItsParentResumes() {
        TableModel child = table("Orders", 100);
        TableModel parent = table("Customers", 100);
        progress.stage("profile", List.of(parent, child));
        advance(600);
        Runnable childRow = progress.started(child);
        advance(300);
        Runnable parentRow = progress.started(parent);
        parentRow.run();
        advance(300);
        childRow.run();
        assertThat(drawn())
                .containsExactly(
                        "profile 2/2 Orders  0 / 100 rows  0%",
                        "profile 1/2 Customers  0 / 100 rows  0%", "profile 2/2 Orders  1 / 100 rows  1%");
    }

    @Test
    void offObservesNothing() {
        Progress off = Progress.off();
        assertThat(off.enabled()).isFalse();
        assertThat(off.listener()).isSameAs(ReadListener.NONE);
        off.stage("write", List.of(table("T", 1)));
        off.close();
    }
}
