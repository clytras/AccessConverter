package com.lytrax.accessconverter.fixtures;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

import com.healthmarketscience.jackcess.Database;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;

/**
 * Tier B: Access-authored databases (Access 97 to 2019) from the Jackcess test suite, Apache-2.0. They're downloaded
 * on first use into {@code target/fixtures/jackcess/} from a pinned commit and verified against pinned SHA-256
 * checksums; nothing is committed. For offline builds, {@code -Dfixtures.jackcess.baseUrl=file:///…/data/} points
 * at a local checkout of {@code src/test/resources/data/}.
 */
public enum JackcessCorpus {
    INDEX_V1997("V1997/indexV1997.mdb", 139_264, "38b5db56a09d35a184cc0a8e54587a3cd9b3c89ba87d6735267323af63c42349"),
    INDEX_V2000("V2000/indexV2000.mdb", 294_912, "4778d1d4b46997457fdb17e224d8fa8b52fef48925cb526e08aad26ba7d97dc9"),
    REF_GLOBAL_V2000(
            "V2000/refGlobalV2000.mdb", 3_043_328, "7e141820883fe521684555574807d9263a53a35452336b53be30004861d3d447"),
    UNICODE_COMP_V2003(
            "V2003/unicodeCompV2003.mdb", 294_912, "44533b58271b668c3fe6d342aeb9931013f4f0c948b014f8778541ddf3dc0fa2"),
    BLOB_V2007("V2007/blobV2007.accdb", 8_634_368, "279bccf38b4414ca98ac3ea4c6aca41daba697050cccaa7271e20021c15c827c"),
    /** Has a linked table ({@code Table2}) whose target is {@link #LINKEE_TEST}. */
    LINKED_V2007(
            "V2007/linkedV2007.accdb", 454_656, "f3ec2467ed9d6620a97ff782eec55371e50290eeaa3d2e952d6480db2a7a458f"),
    OLD_DATES_V2007(
            "V2007/oldDatesV2007.accdb", 483_328, "31326224eb7bb75154dfcd6921bf83d5e6d9af828e8af4a91ea3b959468fad59"),
    BIG_INDEX_V2010(
            "V2010/bigIndexV2010.accdb", 360_448, "33266e4eb0a6de44f33c427983c8dd2f558f7a9ae3c52beea77e08cc0b22be98"),
    CALC_FIELD_V2010(
            "V2010/calcFieldV2010.accdb", 712_704, "bf95642c1e661b7fb7f615124bbb8dcc614c539fff080947ae24a1e3d6ea07dd"),
    COMMON1_V2010(
            "V2010/common1V2010.accdb", 446_464, "3cf2ff01454374abce0ee2203b89d166fe0120fb4f235894c0bbacce2abe8e18"),
    COMP_INDEX_V2010(
            "V2010/compIndexV2010.accdb", 380_928, "a3cb942fbd613c9fd8b922cd90029ef8089b243608b3b46c5ca28e1b668eca06"),
    COMPLEX_DATA_V2010(
            "V2010/complexDataV2010.accdb",
            544_768,
            "c9707ec54fd4f712c74ee3638963ad203fc60340f5bb7ad0c86bc2878a91e6df"),
    EMOTICONS_V2010(
            "V2010/emoticonsV2010.accdb", 385_024, "5ef1b0d7a892f6ddc0bef54c44b4861b7df1230c3b4023d9583fcf13d4bf62b4"),
    FIXED_NUMERIC_V2010(
            "V2010/fixedNumericV2010.accdb",
            380_928,
            "a1b1e722c6ebdcab2827ca8e64e37a23539068e4e2d88554294b3fb0ac349e80"),
    FIXED_TEXT_V2010(
            "V2010/fixedTextV2010.accdb", 380_928, "a420710da092de30f1de71aa43e1fb24b0b7268ccba71dda8d866f01011e119f"),
    /** Every character class Access sorts, 133,792 rows. */
    INDEX_CODES_V2010(
            "V2010/indexCodesV2010.accdb",
            11_112_448,
            "315198bbb5081bc6e54c2986bfc9aee48873ca5e64b51fabc6032cc955350a1f"),
    INDEX_PROPERTIES_V2010(
            "V2010/indexPropertiesV2010.accdb",
            491_520,
            "7f4d19744202b30da79a32f4fc9c0792610e76bf6d71334ae011d24e24934254"),
    INDEX_V2010("V2010/indexV2010.accdb", 483_328, "67cfa2706fba8bba2699c38b760db37e38b88c83e617e1d8ec0361dc7f0d3a76"),
    EXT_DATE_V2019(
            "V2019/extDateV2019.accdb", 557_056, "bb6d7af8e5275c1143b5f68447b2f96bd11e460fbec6856de7b3e339ccb53cf6"),
    /** Target of {@link #LINKED_V2007}'s linked table, for {@code --linked resolve}. */
    LINKEE_TEST("linkeeTest.accdb", 524_288, "1e55f525123a1f49866dc86ff3bcaf31553f3960b991933c92efa1ca4f840307");

    /** The spannm/jackcess commit the files are pinned to (2026-09-19). */
    public static final String COMMIT = "29610dfd8a13e27bc1aa70ec1e38586bff3633b2";

    private static final String BASE_URL =
            "https://raw.githubusercontent.com/spannm/jackcess/" + COMMIT + "/src/test/resources/data/";
    private static final int ATTEMPTS = 3;

    private final String repoPath;
    private final long size;
    private final String sha256;
    private Path verified;

    JackcessCorpus(String repoPath, long size, String sha256) {
        this.repoPath = repoPath;
        this.size = size;
        this.sha256 = sha256;
    }

    public String fileName() {
        return repoPath.substring(repoPath.lastIndexOf('/') + 1);
    }

    /** The verified local copy, downloaded first if it's missing or doesn't match its checksum. */
    public synchronized Path path() {
        if (verified == null) {
            try {
                Path file = Fixtures.root().resolve("jackcess").resolve(fileName());
                if (!Checksums.matches(file, size, sha256)) {
                    download(file);
                }
                verified = file;
            } catch (IOException e) {
                throw new UncheckedIOException("fetching Jackcess test database " + repoPath, e);
            }
        }
        return verified;
    }

    public Database open() throws IOException {
        return Fixtures.openReadOnly(path());
    }

    private void download(Path file) throws IOException {
        URI source = URI.create(System.getProperty("fixtures.jackcess.baseUrl", BASE_URL))
                .resolve(repoPath);
        Path partial = Files.createDirectories(file.getParent()).resolve(fileName() + ".partial");
        for (int attempt = 1; ; attempt++) {
            try {
                fetch(source, partial);
                break;
            } catch (IOException e) {
                if (attempt == ATTEMPTS) {
                    throw new IOException("downloading " + source + " failed " + ATTEMPTS + " times", e);
                }
                pause(Duration.ofSeconds(2L * attempt));
            }
        }
        // The commit is pinned, so a mismatch is corruption or tampering, never a newer upstream version
        if (!Checksums.matches(partial, size, sha256)) {
            String actual = Checksums.sha256(partial);
            long actualSize = Files.size(partial);
            Files.delete(partial);
            throw new IOException("checksum mismatch for " + source + ": expected " + size + " bytes, SHA-256 " + sha256
                    + "; got " + actualSize + " bytes, SHA-256 " + actual);
        }
        Files.move(partial, file, REPLACE_EXISTING, ATOMIC_MOVE);
    }

    private static void fetch(URI source, Path target) throws IOException {
        if ("file".equals(source.getScheme())) {
            Files.copy(Path.of(source), target, REPLACE_EXISTING);
            return;
        }
        try (HttpClient client = HttpClient.newBuilder()
                .followRedirects(HttpClient.Redirect.NORMAL)
                .connectTimeout(Duration.ofSeconds(30))
                .build()) {
            HttpRequest request = HttpRequest.newBuilder(source)
                    .timeout(Duration.ofMinutes(5))
                    .build();
            HttpResponse<Path> response = client.send(request, HttpResponse.BodyHandlers.ofFile(target));
            if (response.statusCode() != 200) {
                throw new IOException("HTTP " + response.statusCode() + " for " + source);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new InterruptedIOException("interrupted while downloading " + source);
        }
    }

    private static void pause(Duration duration) throws InterruptedIOException {
        try {
            Thread.sleep(duration);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new InterruptedIOException("interrupted while waiting to retry");
        }
    }
}
