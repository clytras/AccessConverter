package io.lytrax.accessconverter.profile;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

/**
 * Which characters of a key's text the default MySQL and MariaDB collations ({@code utf8mb4_0900_as_ci},
 * {@code utf8mb4_uca1400_as_ci}) compare exactly as Access does, so a unique key that holds in Access holds there
 * too (05, Collation).
 *
 * <p>The set is data, not a list anyone maintains: {@code key-text-weights.tsv} records each candidate character's
 * weight under both collations as the servers report it, and whether it is safe, which a test derives from those
 * weights and Access's own index encoding. Outside it the collations are stricter than Access: kana variants, space
 * and no-break space, control and format characters, {@code ª}/{@code º}, the ohm sign and omega all compare equal
 * there and distinct in Access. Text holding anything else is compared with {@code utf8mb4_bin} instead.
 */
public final class KeyText {

    /** The data file, next to this class. */
    public static final String RESOURCE = "key-text-weights.tsv";

    /** One line of the data file. */
    public record Weight(int codePoint, String mysql, String mariadb, boolean safe) {}

    private static final List<Weight> WEIGHTS = load();

    private static final BitSet SAFE = new BitSet(0x10000);

    static {
        WEIGHTS.stream().filter(Weight::safe).forEach(w -> SAFE.set(w.codePoint()));
    }

    private KeyText() {}

    /** Whether every character of the text is one the default collations compare as Access does. */
    public static boolean isSafe(String text) {
        for (int i = 0; i < text.length(); i++) {
            if (!SAFE.get(text.charAt(i))) {
                return false; // a surrogate is never set, so a character outside the BMP is never safe
            }
        }
        return true;
    }

    /** Every candidate character with its recorded weights, in code point order. */
    public static List<Weight> weights() {
        return WEIGHTS;
    }

    private static List<Weight> load() {
        try (InputStream in = KeyText.class.getResourceAsStream(RESOURCE)) {
            if (in == null) {
                throw new IllegalStateException(RESOURCE + " is missing from the build");
            }
            List<Weight> weights = new ArrayList<>();
            BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
            for (String line = reader.readLine(); line != null; line = reader.readLine()) {
                if (line.isBlank() || line.startsWith("#")) {
                    continue;
                }
                String[] fields = line.split("\t", -1);
                weights.add(new Weight(Integer.parseInt(fields[0], 16), fields[1], fields[2], "1".equals(fields[3])));
            }
            return List.copyOf(weights);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
