package com.lytrax.accessconverter.extract;

import com.lytrax.accessconverter.model.CheckRule;
import com.lytrax.accessconverter.model.DefaultValue;
import com.lytrax.accessconverter.model.expr.Expr;
import com.lytrax.accessconverter.model.expr.Expr.And;
import com.lytrax.accessconverter.model.expr.Expr.Between;
import com.lytrax.accessconverter.model.expr.Expr.BooleanLiteral;
import com.lytrax.accessconverter.model.expr.Expr.ColumnRef;
import com.lytrax.accessconverter.model.expr.Expr.Comparison;
import com.lytrax.accessconverter.model.expr.Expr.CurrentDateTime;
import com.lytrax.accessconverter.model.expr.Expr.DateTimeLiteral;
import com.lytrax.accessconverter.model.expr.Expr.In;
import com.lytrax.accessconverter.model.expr.Expr.IsNull;
import com.lytrax.accessconverter.model.expr.Expr.Like;
import com.lytrax.accessconverter.model.expr.Expr.NewGuid;
import com.lytrax.accessconverter.model.expr.Expr.NewRandomId;
import com.lytrax.accessconverter.model.expr.Expr.Not;
import com.lytrax.accessconverter.model.expr.Expr.NullLiteral;
import com.lytrax.accessconverter.model.expr.Expr.NumberLiteral;
import com.lytrax.accessconverter.model.expr.Expr.Operator;
import com.lytrax.accessconverter.model.expr.Expr.Or;
import com.lytrax.accessconverter.model.expr.Expr.StringLiteral;
import com.lytrax.accessconverter.model.expr.LikePattern;
import java.math.BigDecimal;
import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parses Access default values and validation rules into {@link Expr}, within the closed subset of 04
 * (Expressions). Anything else becomes an unsupported result carrying the reason: never a guess.
 */
public final class ExpressionTranslator {
    private ExpressionTranslator() {}

    /** A {@code DefaultValue}: a single value, optionally written with a leading {@code =}. */
    public static DefaultValue translateDefault(String raw) {
        try {
            Parser parser = new Parser(raw, null);
            parser.skipLeadingEquals();
            Expr expr = parser.parseAll();
            requireValue(expr);
            return new DefaultValue(raw, expr, null);
        } catch (Unsupported e) {
            return new DefaultValue(raw, null, e.getMessage());
        }
    }

    /** A column's {@code ValidationRule}. A leading operator applies to the column, as in {@code >0}. */
    public static CheckRule translateColumnRule(String raw, String validationText, String column) {
        return translateRule(raw, validationText, column);
    }

    /** A table's {@code ValidationRule}, which names its columns, as in {@code [EndDate]>=[StartDate]}. */
    public static CheckRule translateTableRule(String raw, String validationText) {
        return translateRule(raw, validationText, null);
    }

    private static CheckRule translateRule(String raw, String validationText, String column) {
        try {
            Expr expr = new Parser(raw, column).parseAll();
            requireCondition(expr);
            return new CheckRule(raw, validationText, expr, null);
        } catch (Unsupported e) {
            return new CheckRule(raw, validationText, null, e.getMessage());
        }
    }

    private static void requireValue(Expr expr) {
        switch (expr) {
            case NullLiteral n -> {}
            case BooleanLiteral b -> {}
            case NumberLiteral n -> {}
            case StringLiteral s -> {}
            case DateTimeLiteral d -> {}
            case CurrentDateTime c -> {}
            case NewGuid g -> {}
            case NewRandomId r -> {}
            case ColumnRef c -> throw new Unsupported("a default can't refer to a column");
            default -> throw new Unsupported("a default must be a single value");
        }
    }

    /** A rule must be a condition over its operands, and deterministic: a CHECK can't call Now() or GenGUID(). */
    private static void requireCondition(Expr expr) {
        switch (expr) {
            case And and -> {
                requireCondition(and.left());
                requireCondition(and.right());
            }
            case Or or -> {
                requireCondition(or.left());
                requireCondition(or.right());
            }
            case Not not -> requireCondition(not.operand());
            case Comparison c -> requireDeterministic(c);
            case Between b -> requireDeterministic(b);
            case In in -> requireDeterministic(in);
            case IsNull n -> requireDeterministic(n);
            case Like l -> requireDeterministic(l);
            default -> throw new Unsupported("a validation rule must be a condition");
        }
    }

    private static void requireDeterministic(Expr expr) {
        switch (expr) {
            case CurrentDateTime c -> throw new Unsupported("Now(), Date() and Time() make a rule non-deterministic");
            case NewGuid g -> throw new Unsupported("GenGUID() makes a rule non-deterministic");
            case NewRandomId r -> throw new Unsupported("GenUniqueID() makes a rule non-deterministic");
            case And a -> {
                requireDeterministic(a.left());
                requireDeterministic(a.right());
            }
            case Or o -> {
                requireDeterministic(o.left());
                requireDeterministic(o.right());
            }
            case Not n -> requireDeterministic(n.operand());
            case Comparison c -> {
                requireDeterministic(c.left());
                requireDeterministic(c.right());
            }
            case Between b -> {
                requireDeterministic(b.operand());
                requireDeterministic(b.low());
                requireDeterministic(b.high());
            }
            case In in -> {
                requireDeterministic(in.operand());
                in.values().forEach(ExpressionTranslator::requireDeterministic);
            }
            case IsNull n -> requireDeterministic(n.operand());
            case Like l -> requireDeterministic(l.operand());
            default -> {}
        }
    }

    /** An expression outside the supported subset; the message says which construct. */
    static final class Unsupported extends RuntimeException {
        private static final long serialVersionUID = 1L;

        Unsupported(String reason) {
            super(reason, null, false, false);
        }
    }

    private enum Kind {
        NUMBER,
        STRING,
        DATE,
        IDENT,
        BRACKET,
        SYMBOL,
        END
    }

    private record Token(Kind kind, String text) {
        boolean is(String symbolOrKeyword) {
            return (kind == Kind.SYMBOL || kind == Kind.IDENT) && text.equalsIgnoreCase(symbolOrKeyword);
        }

        String describe() {
            return kind == Kind.END ? "end of expression" : "'" + text + "'";
        }
    }

    private static final class Lexer {
        private static final Pattern NUMBER = Pattern.compile("(?:\\d+(?:\\.\\d*)?|\\.\\d+)(?:[eE][+-]?\\d+)?");
        private static final Set<String> TWO_CHAR = Set.of("<>", "<=", ">=");
        private static final String ONE_CHAR = "=<>(),+-*/&^\\!.";

        static List<Token> tokenize(String text) {
            List<Token> tokens = new ArrayList<>();
            int i = 0;
            while (i < text.length()) {
                char c = text.charAt(i);
                if (Character.isWhitespace(c)) {
                    i++;
                } else if (c == '"' || c == '\'') {
                    i = string(text, i, c, tokens);
                } else if (c == '#') {
                    int end = text.indexOf('#', i + 1);
                    if (end < 0) {
                        throw new Unsupported("unterminated date literal");
                    }
                    tokens.add(new Token(Kind.DATE, text.substring(i + 1, end)));
                    i = end + 1;
                } else if (c == '[') {
                    int end = text.indexOf(']', i + 1);
                    if (end < 0) {
                        throw new Unsupported("unterminated [name]");
                    }
                    String name = text.substring(i + 1, end);
                    if (name.isBlank()) {
                        throw new Unsupported("empty [name]");
                    }
                    tokens.add(new Token(Kind.BRACKET, name));
                    i = end + 1;
                } else if (Character.isDigit(c)
                        || (c == '.' && i + 1 < text.length() && Character.isDigit(text.charAt(i + 1)))) {
                    Matcher m = NUMBER.matcher(text).region(i, text.length());
                    if (!m.lookingAt()) {
                        throw new Unsupported("malformed number");
                    }
                    tokens.add(new Token(Kind.NUMBER, m.group()));
                    i = m.end();
                } else if (Character.isLetter(c) || c == '_') {
                    int start = i;
                    while (i < text.length() && (Character.isLetterOrDigit(text.charAt(i)) || text.charAt(i) == '_')) {
                        i++;
                    }
                    tokens.add(new Token(Kind.IDENT, text.substring(start, i)));
                } else if (i + 1 < text.length() && TWO_CHAR.contains(text.substring(i, i + 2))) {
                    tokens.add(new Token(Kind.SYMBOL, text.substring(i, i + 2)));
                    i += 2;
                } else if (ONE_CHAR.indexOf(c) >= 0) {
                    tokens.add(new Token(Kind.SYMBOL, String.valueOf(c)));
                    i++;
                } else {
                    throw new Unsupported("unexpected character '" + c + "'");
                }
            }
            tokens.add(new Token(Kind.END, ""));
            return tokens;
        }

        /** Access strings double their quote character to escape it: {@code "say ""hi"""}. */
        private static int string(String text, int start, char quote, List<Token> tokens) {
            StringBuilder value = new StringBuilder();
            int i = start + 1;
            while (true) {
                if (i >= text.length()) {
                    throw new Unsupported("unterminated string");
                }
                char c = text.charAt(i);
                if (c == quote) {
                    if (i + 1 < text.length() && text.charAt(i + 1) == quote) {
                        value.append(quote);
                        i += 2;
                        continue;
                    }
                    tokens.add(new Token(Kind.STRING, value.toString()));
                    return i + 1;
                }
                value.append(c);
                i++;
            }
        }
    }

    private static final class Parser {
        private static final Set<String> KEYWORDS = Set.of(
                "and", "or", "not", "xor", "eqv", "imp", "between", "in", "is", "null", "like", "true", "false", "yes",
                "no", "on", "off", "mod");
        private static final Set<String> COMPARISONS = Set.of("=", "<>", "<", "<=", ">", ">=");
        private static final Set<String> ARITHMETIC = Set.of("+", "-", "*", "/", "\\", "^", "&", "mod");

        private final List<Token> tokens;
        private final String implicitColumn;
        private int pos;

        Parser(String text, String implicitColumn) {
            if (text == null || text.isBlank()) {
                throw new Unsupported("empty expression");
            }
            this.tokens = Lexer.tokenize(text);
            this.implicitColumn = implicitColumn;
        }

        void skipLeadingEquals() {
            if (peek().is("=")) {
                pos++;
            }
        }

        Expr parseAll() {
            Expr expr = or();
            if (peek().kind != Kind.END) {
                Token t = peek();
                if (ARITHMETIC.contains(t.text.toLowerCase(Locale.ROOT))) {
                    throw new Unsupported("arithmetic and concatenation are outside the supported subset");
                }
                throw new Unsupported("unexpected " + t.describe());
            }
            return expr;
        }

        private Expr or() {
            Expr left = and();
            while (true) {
                if (peek().is("or")) {
                    pos++;
                    left = new Or(left, and());
                } else if (peek().is("xor") || peek().is("eqv") || peek().is("imp")) {
                    throw new Unsupported("operator " + peek().text + " is outside the supported subset");
                } else {
                    return left;
                }
            }
        }

        private Expr and() {
            Expr left = not();
            while (peek().is("and")) {
                pos++;
                left = new And(left, not());
            }
            return left;
        }

        private Expr not() {
            if (peek().is("not") && !(peek(1).is("like") || peek(1).is("between") || peek(1).is("in"))) {
                pos++;
                return new Not(not());
            }
            return predicate();
        }

        private Expr predicate() {
            Expr left = startsPredicateTail() ? implicit() : operand();
            Expr result = tail(left);
            if (result != left && startsPredicateTail()) {
                throw new Unsupported("chained comparisons are outside the supported subset");
            }
            return result;
        }

        private boolean startsPredicateTail() {
            Token t = peek();
            return (t.kind == Kind.SYMBOL && COMPARISONS.contains(t.text))
                    || t.is("between")
                    || t.is("in")
                    || t.is("is")
                    || t.is("like")
                    || (t.is("not") && (peek(1).is("like") || peek(1).is("between") || peek(1).is("in")));
        }

        private Expr implicit() {
            if (implicitColumn == null) {
                throw new Unsupported("a comparison needs a left operand here");
            }
            return new ColumnRef(implicitColumn);
        }

        private Expr tail(Expr left) {
            Token t = peek();
            if (t.kind == Kind.SYMBOL && COMPARISONS.contains(t.text)) {
                pos++;
                return new Comparison(operator(t.text), left, operand());
            }
            boolean negated = false;
            if (t.is("not")) {
                if (!(peek(1).is("like") || peek(1).is("between") || peek(1).is("in"))) {
                    throw new Unsupported("unexpected 'Not' after an operand");
                }
                negated = true;
                pos++;
                t = peek();
            }
            if (t.is("between")) {
                pos++;
                Expr low = operand();
                expect("and");
                return new Between(left, low, operand(), negated);
            }
            if (t.is("in")) {
                pos++;
                expect("(");
                List<Expr> values = new ArrayList<>();
                values.add(operand());
                while (peek().is(",")) {
                    pos++;
                    values.add(operand());
                }
                expect(")");
                return new In(left, values, negated);
            }
            if (t.is("like")) {
                pos++;
                if (!(operand() instanceof StringLiteral pattern)) {
                    throw new Unsupported("Like needs a string pattern");
                }
                LikePattern parsed = LikePattern.parse(pattern.value())
                        .orElseThrow(() -> new Unsupported("Like character classes [...] are outside the subset"));
                return new Like(left, parsed, negated);
            }
            if (t.is("is")) {
                pos++;
                boolean isNot = false;
                if (peek().is("not")) {
                    isNot = true;
                    pos++;
                }
                expect("null");
                return new IsNull(left, isNot);
            }
            return left;
        }

        private Expr operand() {
            Token t = peek();
            if (t.is("-") || t.is("+")) {
                pos++;
                Token number = peek();
                if (number.kind != Kind.NUMBER) {
                    throw new Unsupported("arithmetic and concatenation are outside the supported subset");
                }
                pos++;
                BigDecimal value = new BigDecimal(number.text);
                return noArithmetic(new NumberLiteral(t.is("-") ? value.negate() : value));
            }
            return noArithmetic(primary());
        }

        private Expr noArithmetic(Expr operand) {
            Token t = peek();
            if (t.kind == Kind.SYMBOL && ARITHMETIC.contains(t.text) || t.is("mod")) {
                throw new Unsupported("arithmetic and concatenation are outside the supported subset");
            }
            return operand;
        }

        private Expr primary() {
            Token t = next();
            switch (t.kind) {
                case NUMBER:
                    return new NumberLiteral(new BigDecimal(t.text));
                case STRING:
                    return new StringLiteral(t.text);
                case DATE:
                    return AccessDates.parse(t.text);
                case BRACKET:
                    if (peek().is(".") || peek().is("!")) {
                        throw new Unsupported("qualified references are outside the supported subset");
                    }
                    return new ColumnRef(t.text);
                case SYMBOL:
                    if (t.text.equals("(")) {
                        Expr inner = or();
                        expect(")");
                        return inner;
                    }
                    throw new Unsupported("unexpected " + t.describe());
                case IDENT:
                    return identifier(t);
                default:
                    throw new Unsupported("unexpected " + t.describe());
            }
        }

        private Expr identifier(Token t) {
            String word = t.text.toLowerCase(Locale.ROOT);
            if (peek().is("(")) {
                pos++;
                if (!peek().is(")")) {
                    throw new Unsupported("function " + t.text + "(…) with arguments is outside the supported subset");
                }
                pos++;
                return switch (word) {
                    case "now" -> new CurrentDateTime(CurrentDateTime.Part.NOW);
                    case "date" -> new CurrentDateTime(CurrentDateTime.Part.DATE);
                    case "time" -> new CurrentDateTime(CurrentDateTime.Part.TIME);
                    case "genguid" -> new NewGuid();
                    case "genuniqueid" -> new NewRandomId();
                    default -> throw new Unsupported("function " + t.text + "() is outside the supported subset");
                };
            }
            return switch (word) {
                case "true", "yes", "on" -> new BooleanLiteral(true);
                case "false", "no", "off" -> new BooleanLiteral(false);
                case "null" -> new NullLiteral();
                default -> {
                    if (KEYWORDS.contains(word)) {
                        throw new Unsupported("unexpected " + t.describe());
                    }
                    if (peek().is(".") || peek().is("!")) {
                        throw new Unsupported("qualified references are outside the supported subset");
                    }
                    yield new ColumnRef(t.text);
                }
            };
        }

        private static Operator operator(String symbol) {
            return switch (symbol) {
                case "=" -> Operator.EQ;
                case "<>" -> Operator.NE;
                case "<" -> Operator.LT;
                case "<=" -> Operator.LE;
                case ">" -> Operator.GT;
                case ">=" -> Operator.GE;
                default -> throw new IllegalArgumentException(symbol);
            };
        }

        private void expect(String symbolOrKeyword) {
            Token t = next();
            if (!t.is(symbolOrKeyword)) {
                throw new Unsupported("expected '" + symbolOrKeyword + "' but found " + t.describe());
            }
        }

        private Token peek() {
            return peek(0);
        }

        private Token peek(int ahead) {
            return tokens.get(Math.min(pos + ahead, tokens.size() - 1));
        }

        private Token next() {
            Token t = peek();
            if (t.kind != Kind.END) {
                pos++;
            }
            return t;
        }
    }

    /**
     * Access date literals, always read in US order: {@code #1/31/2000#}, {@code #2000-01-31 10:00#},
     * {@code #10:30 PM#}. A time without a date is on Access's day zero, 1899-12-30. Two-digit years depend on
     * Access's century window and are unsupported rather than guessed.
     */
    static final class AccessDates {
        static final LocalDate DAY_ZERO = LocalDate.of(1899, 12, 30);
        private static final Pattern US = Pattern.compile("(\\d{1,2})/(\\d{1,2})/(\\d+)");
        private static final Pattern ISO = Pattern.compile("(\\d{4})-(\\d{1,2})-(\\d{1,2})");
        private static final Pattern TIME = Pattern.compile("(\\d{1,2}):(\\d{2})(?::(\\d{2}))?(?:\\s*([AaPp][Mm]))?");

        private AccessDates() {}

        static DateTimeLiteral parse(String text) {
            String[] parts = text.strip().split("\\s+", 2);
            try {
                LocalDate date = date(parts[0]);
                if (date == null) {
                    return new DateTimeLiteral(DAY_ZERO.atTime(time(text.strip())), false, true);
                }
                if (parts.length == 1) {
                    return new DateTimeLiteral(date.atStartOfDay(), true, false);
                }
                return new DateTimeLiteral(LocalDateTime.of(date, time(parts[1])), true, true);
            } catch (DateTimeException e) {
                throw new Unsupported("invalid date literal #" + text + "#");
            }
        }

        private static LocalDate date(String text) {
            Matcher us = US.matcher(text);
            if (us.matches()) {
                return LocalDate.of(year(us.group(3)), Integer.parseInt(us.group(1)), Integer.parseInt(us.group(2)));
            }
            Matcher iso = ISO.matcher(text);
            if (iso.matches()) {
                return LocalDate.of(
                        Integer.parseInt(iso.group(1)), Integer.parseInt(iso.group(2)), Integer.parseInt(iso.group(3)));
            }
            return null;
        }

        private static int year(String digits) {
            if (digits.length() != 4) {
                throw new Unsupported("two-digit and other non-four-digit years are outside the supported subset");
            }
            return Integer.parseInt(digits);
        }

        private static LocalTime time(String text) {
            Matcher m = TIME.matcher(text);
            if (!m.matches()) {
                throw new Unsupported("unrecognized date literal #" + text + "#");
            }
            int hour = Integer.parseInt(m.group(1));
            int minute = Integer.parseInt(m.group(2));
            int second = m.group(3) == null ? 0 : Integer.parseInt(m.group(3));
            String meridiem = m.group(4);
            if (meridiem != null) {
                if (hour < 1 || hour > 12) {
                    throw new Unsupported("invalid 12-hour time #" + text + "#");
                }
                boolean pm = meridiem.equalsIgnoreCase("pm");
                hour = hour % 12 + (pm ? 12 : 0);
            }
            return LocalTime.of(hour, minute, second);
        }
    }
}
