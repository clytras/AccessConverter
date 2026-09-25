package io.lytrax.accessconverter.extract;

import java.util.function.Predicate;

/** @param tables which tables to include, by name; relationships that touch an excluded table are skipped */
public record ExtractOptions(Predicate<String> tables) {
    public static final ExtractOptions ALL = new ExtractOptions(name -> true);
}
