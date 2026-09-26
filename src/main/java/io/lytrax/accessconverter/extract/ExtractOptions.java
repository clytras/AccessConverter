package io.lytrax.accessconverter.extract;

import java.util.function.Predicate;

/**
 * @param tables which tables to include, by name; relationships that touch an excluded table are skipped
 * @param continueOnTableError a linked table that {@code --linked resolve} can't read is reported as an error and left
 *     unread ({@code --on-table-error continue}), instead of failing the extraction
 */
public record ExtractOptions(Predicate<String> tables, boolean continueOnTableError) {
    public static final ExtractOptions ALL = new ExtractOptions(name -> true);

    public ExtractOptions(Predicate<String> tables) {
        this(tables, false);
    }

    public ExtractOptions continueOnTableError(boolean continueOnError) {
        return new ExtractOptions(tables, continueOnError);
    }
}
