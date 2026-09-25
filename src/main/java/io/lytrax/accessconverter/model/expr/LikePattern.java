package io.lytrax.accessconverter.model.expr;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;

/**
 * An Access {@code Like} pattern: {@code *} any run of characters, {@code ?} one character, {@code #} one digit.
 * Character classes ({@code [...]}) are outside the supported subset.
 */
public record LikePattern(String access, List<Element> elements) {

    public sealed interface Element {}

    public record Literal(String text) implements Element {}

    public record AnyString() implements Element {}

    public record AnyChar() implements Element {}

    public record AnyDigit() implements Element {}

    public LikePattern {
        Objects.requireNonNull(access, "access");
        elements = List.copyOf(elements);
    }

    /** Parses an Access pattern, or returns empty when it uses a character class. */
    public static Optional<LikePattern> parse(String access) {
        List<Element> elements = new ArrayList<>();
        StringBuilder literal = new StringBuilder();
        for (int i = 0; i < access.length(); i++) {
            char c = access.charAt(i);
            if (c == '[' || c == ']') {
                return Optional.empty();
            }
            Element wildcard =
                    switch (c) {
                        case '*' -> new AnyString();
                        case '?' -> new AnyChar();
                        case '#' -> new AnyDigit();
                        default -> null;
                    };
            if (wildcard == null) {
                literal.append(c);
                continue;
            }
            if (!literal.isEmpty()) {
                elements.add(new Literal(literal.toString()));
                literal.setLength(0);
            }
            elements.add(wildcard);
        }
        if (!literal.isEmpty()) {
            elements.add(new Literal(literal.toString()));
        }
        return Optional.of(new LikePattern(access, elements));
    }

    /** True when SQL {@code LIKE} can express it: {@code #} has no {@code LIKE} equivalent. */
    public boolean mapsToSqlLike() {
        return elements.stream().noneMatch(e -> e instanceof AnyDigit);
    }

    /** A Java regex matching like Access: the whole value, case-insensitive for every letter. */
    public Pattern toRegex() {
        return toRegex(true);
    }

    /**
     * A Java regex matching the whole value.
     *
     * @param unicodeCase fold every letter's case (Access), or ASCII letters only (SQLite's {@code LIKE})
     */
    public Pattern toRegex(boolean unicodeCase) {
        StringBuilder regex = new StringBuilder();
        for (Element e : elements) {
            switch (e) {
                case Literal l -> regex.append(Pattern.quote(l.text()));
                case AnyString s -> regex.append(".*");
                case AnyChar c -> regex.append('.');
                case AnyDigit d -> regex.append("[0-9]");
            }
        }
        return Pattern.compile(
                regex.toString(), Pattern.CASE_INSENSITIVE | (unicodeCase ? Pattern.UNICODE_CASE : 0) | Pattern.DOTALL);
    }
}
