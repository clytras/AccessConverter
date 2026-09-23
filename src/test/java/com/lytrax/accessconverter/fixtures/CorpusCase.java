package com.lytrax.accessconverter.fixtures;

import com.lytrax.accessconverter.source.OpenOptions;
import java.nio.file.Path;
import java.util.stream.Stream;

/**
 * One database of tiers A, B and D, for parameterized tests: named by its id, opened with its test password.
 */
public record CorpusCase(String id, Path file, OpenOptions options) {

    /** Every database of tiers A, B and D (tier B's non-databases excluded). */
    public static Stream<CorpusCase> databases() {
        return Stream.of(
                        Stream.of(GeneratedFixture.values())
                                .map(f -> new CorpusCase("generated/" + f.fileName(), f.path(), OpenOptions.DEFAULT)),
                        CorpusFile.databases().stream().map(f -> new CorpusCase(f.id(), f.file(), f.openOptions())),
                        Stream.of(Access97Fixture.values())
                                .map(f -> new CorpusCase(
                                        "access97/" + f.file().getFileName(), f.file(), f.openOptions())))
                .flatMap(s -> s);
    }

    @Override
    public String toString() {
        return id;
    }
}
