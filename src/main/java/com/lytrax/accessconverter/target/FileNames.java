package com.lytrax.accessconverter.target;

import java.text.Normalizer;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * File names made from data (table names now, attachment and package names in 08's files mode), safe on Windows,
 * macOS and Linux: no directory parts, no characters any of them rejects, no Windows device names, at most
 * {@value #MAX_LENGTH} characters, and never two names that one of those file systems would take for the same file.
 * A collision gets a {@code -<n>} suffix, never an overwrite (08, File-name safety).
 */
public final class FileNames {

    public static final int MAX_LENGTH = 200;

    /** Characters Windows rejects in a name, plus the path separators of every OS. */
    private static final Pattern INVALID = Pattern.compile("[<>:\"/\\\\|?*\\x00-\\x1F\\x7F]");

    private static final Pattern DEVICE = Pattern.compile("(?i)(CON|PRN|AUX|NUL|COM[0-9¹²³]|LPT[0-9¹²³])(\\..*)?");

    /** The names handed out so far, as a case-insensitive, normalization-insensitive file system compares them. */
    private final Set<String> taken = new HashSet<>();

    /**
     * A unique, safe file name for {@code name} with {@code extension} (which includes its dot).
     *
     * @param name the data's own name; it may hold anything
     */
    public String allocate(String name, String extension) {
        String base = sanitize(name, MAX_LENGTH - extension.length() - 4);
        String candidate = base + extension;
        for (int n = 2; !taken.add(key(candidate)); n++) {
            candidate = base + "-" + n + extension;
        }
        return candidate;
    }

    /** {@code name} as a safe base name of at most {@code maxLength} characters (without a surrogate cut in two). */
    static String sanitize(String name, int maxLength) {
        String text = Normalizer.normalize(name, Normalizer.Form.NFC);
        int slash = Math.max(text.lastIndexOf('/'), text.lastIndexOf('\\'));
        if (slash >= 0 && slash < text.length() - 1) {
            text = text.substring(slash + 1); // directory parts, as in ..\..\x.txt
        }
        text = INVALID.matcher(text).replaceAll("_");
        // Windows drops trailing dots and spaces, and a leading dot hides a file on Linux and macOS
        text = text.strip().replaceAll("[. ]+$", "").replaceAll("^\\.+", "_");
        if (text.isEmpty()) {
            text = "_";
        }
        if (DEVICE.matcher(text).matches()) {
            text = "_" + text;
        }
        if (text.length() > maxLength) {
            int end = maxLength;
            if (Character.isHighSurrogate(text.charAt(end - 1))) {
                end--;
            }
            text = text.substring(0, end).replaceAll("[. ]+$", "");
        }
        return text;
    }

    private static String key(String fileName) {
        return Normalizer.normalize(fileName, Normalizer.Form.NFC).toLowerCase(Locale.ROOT);
    }
}
