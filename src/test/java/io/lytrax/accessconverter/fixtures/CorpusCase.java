package io.lytrax.accessconverter.fixtures;

import io.lytrax.accessconverter.source.OpenOptions;
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

    /**
     * The databases with tables linked to another Access file, opened with {@code --linked resolve}: linkedV2007 with
     * its back-end linkeeTest (in another directory, so the link root is given), and the Greek Access 97 pair (in the
     * same directory, the default root).
     */
    public static Stream<CorpusCase> linkedResolved() {
        Path linkee = CorpusFile.get("jackcess/linkeeTest.accdb").file();
        return Stream.of(
                new CorpusCase(
                        "jackcess/V2007/linkedV2007.accdb resolved",
                        CorpusFile.get("jackcess/V2007/linkedV2007.accdb").file(),
                        new OpenOptions(null, null, OpenOptions.Links.resolve(linkee.getParent()))),
                new CorpusCase(
                        "access97/linkFront97.mdb resolved",
                        Access97Fixture.LINK_FRONT.file(),
                        new OpenOptions(null, null, OpenOptions.Links.resolve(null))));
    }

    @Override
    public String toString() {
        return id;
    }
}
