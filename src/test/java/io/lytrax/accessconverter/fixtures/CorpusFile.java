package io.lytrax.accessconverter.fixtures;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

import com.healthmarketscience.jackcess.Database;
import io.lytrax.accessconverter.source.OpenOptions;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.InterruptedIOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tier B (09): third-party test databases listed in {@code fixtures/corpus.tsv}, downloaded on first use into
 * {@code target/fixtures/<source>/} from pinned commits and verified against pinned SHA-256 checksums; nothing is
 * committed. For offline builds, {@code -Dfixtures.baseUrl.<source>=file:///…/} points a source at a local copy.
 *
 * @param path the file's path inside its source's data directory
 * @param password the source project's public test password, or null
 * @param expect {@code ok}, or the {@code SourceException.Kind} the file must fail with
 */
public record CorpusFile(Source source, String path, long size, String sha256, String password, String expect) {

    public enum Source {
        /** spannm/jackcess src/test/resources/data, Apache-2.0: Access 97 to 2019 as Access wrote them. */
        JACKCESS(
                "jackcess",
                "https://raw.githubusercontent.com/spannm/jackcess/29610dfd8a13e27bc1aa70ec1e38586bff3633b2"
                        + "/src/test/resources/data/"),
        /** jahlborn/jackcessencrypt src/test/data, Apache-2.0: encoded, encrypted and Money files. */
        JACKCESS_ENCRYPT(
                "jackcess-encrypt",
                "https://raw.githubusercontent.com/jahlborn/jackcessencrypt/77a1d54db8fc71606f391d94adacae7e34b108ca"
                        + "/src/test/data/"),
        /** andipaetzold/mdb-reader, MIT: Access 2016 files, Office Agile encryption, database passwords. */
        MDB_READER(
                "mdb-reader",
                "https://raw.githubusercontent.com/andipaetzold/mdb-reader/3dc637a9732db79a3bf67506b82a4f83cf07e6a9/");

        private final String id;
        private final String baseUrl;

        Source(String id, String baseUrl) {
            this.id = id;
            this.baseUrl = baseUrl;
        }

        public String id() {
            return id;
        }

        static Source of(String id) {
            for (Source s : values()) {
                if (s.id.equals(id)) {
                    return s;
                }
            }
            throw new IllegalArgumentException("unknown corpus source " + id);
        }
    }

    private static final List<CorpusFile> ALL = load();
    private static final Map<String, Path> VERIFIED = new ConcurrentHashMap<>();
    private static final int ATTEMPTS = 3;

    public CorpusFile {
        Objects.requireNonNull(source, "source");
        Objects.requireNonNull(path, "path");
    }

    public static List<CorpusFile> all() {
        return ALL;
    }

    /** The files that are databases (expected to open). */
    public static List<CorpusFile> databases() {
        return ALL.stream().filter(CorpusFile::isDatabase).toList();
    }

    /** By id, e.g. {@code jackcess/V2007/linkedV2007.accdb}. */
    public static CorpusFile get(String id) {
        return ALL.stream()
                .filter(f -> f.id().equals(id))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("no corpus file " + id));
    }

    public String id() {
        return source.id + "/" + path;
    }

    public String fileName() {
        return path.substring(path.lastIndexOf('/') + 1);
    }

    public boolean isDatabase() {
        return expect.equals("ok");
    }

    public OpenOptions openOptions() {
        return new OpenOptions(password, null);
    }

    /** The verified local copy, downloaded first if it's missing or doesn't match its checksum. */
    public Path file() {
        return VERIFIED.computeIfAbsent(id(), k -> {
            try {
                Path local = Fixtures.root().resolve(source.id).resolve(path);
                if (!Checksums.matches(local, size, sha256)) {
                    download(local);
                }
                return local;
            } catch (IOException e) {
                throw new UncheckedIOException("fetching corpus file " + id(), e);
            }
        });
    }

    /** Opened directly with Jackcess (and the test password), for comparisons with what Jackcess itself reports. */
    public Database open() throws IOException {
        return Fixtures.openReadOnly(file(), password);
    }

    @Override
    public String toString() {
        return id();
    }

    private void download(Path local) throws IOException {
        URI url = URI.create(System.getProperty("fixtures.baseUrl." + source.id, source.baseUrl))
                .resolve(path);
        Path partial = Files.createDirectories(local.getParent()).resolve(fileName() + ".partial");
        for (int attempt = 1; ; attempt++) {
            try {
                fetch(url, partial);
                break;
            } catch (IOException e) {
                if (attempt == ATTEMPTS) {
                    throw new IOException("downloading " + url + " failed " + ATTEMPTS + " times", e);
                }
                pause(Duration.ofSeconds(2L * attempt));
            }
        }
        // The commit is pinned, so a mismatch is corruption or tampering, never a newer upstream version
        if (!Checksums.matches(partial, size, sha256)) {
            String actual = Checksums.sha256(partial);
            long actualSize = Files.size(partial);
            Files.delete(partial);
            throw new IOException("checksum mismatch for " + url + ": expected " + size + " bytes, SHA-256 " + sha256
                    + "; got " + actualSize + " bytes, SHA-256 " + actual);
        }
        Files.move(partial, local, REPLACE_EXISTING, ATOMIC_MOVE);
    }

    private static void fetch(URI url, Path target) throws IOException {
        if ("file".equals(url.getScheme())) {
            Files.copy(Path.of(url), target, REPLACE_EXISTING);
            return;
        }
        try (HttpClient client = HttpClient.newBuilder()
                .followRedirects(HttpClient.Redirect.NORMAL)
                .connectTimeout(Duration.ofSeconds(30))
                .build()) {
            HttpRequest request =
                    HttpRequest.newBuilder(url).timeout(Duration.ofMinutes(5)).build();
            HttpResponse<Path> response = client.send(request, HttpResponse.BodyHandlers.ofFile(target));
            if (response.statusCode() != 200) {
                throw new IOException("HTTP " + response.statusCode() + " for " + url);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new InterruptedIOException("interrupted while downloading " + url);
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

    private static List<CorpusFile> load() {
        List<CorpusFile> files = new ArrayList<>();
        try (BufferedReader in = new BufferedReader(new InputStreamReader(
                Objects.requireNonNull(
                        CorpusFile.class.getResourceAsStream("/fixtures/corpus.tsv"), "fixtures/corpus.tsv"),
                StandardCharsets.UTF_8))) {
            for (String line; (line = in.readLine()) != null; ) {
                if (line.isBlank() || line.startsWith("#")) {
                    continue;
                }
                String[] f = line.split("\t", -1);
                files.add(new CorpusFile(
                        Source.of(f[0]), f[1], Long.parseLong(f[2]), f[3], f[4].equals("-") ? null : f[4], f[5]));
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return List.copyOf(files);
    }
}
