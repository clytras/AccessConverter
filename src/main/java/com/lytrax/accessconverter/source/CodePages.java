package com.lytrax.accessconverter.source;

import java.nio.charset.Charset;
import java.util.Map;
import java.util.Optional;

/**
 * Windows code pages (as Access 97 records them in its header) to Java charsets. Access 97 (Jet 3) stores text in
 * the database's code page; Access 2000 and later store Unicode.
 */
public final class CodePages {
    private static final Map<Integer, String> CHARSETS = Map.ofEntries(
            Map.entry(874, "x-windows-874"),
            Map.entry(932, "windows-31j"),
            Map.entry(936, "GBK"),
            Map.entry(949, "x-windows-949"),
            Map.entry(950, "x-windows-950"),
            Map.entry(1250, "windows-1250"),
            Map.entry(1251, "windows-1251"),
            Map.entry(1252, "windows-1252"),
            Map.entry(1253, "windows-1253"),
            Map.entry(1254, "windows-1254"),
            Map.entry(1255, "windows-1255"),
            Map.entry(1256, "windows-1256"),
            Map.entry(1257, "windows-1257"),
            Map.entry(1258, "windows-1258"),
            Map.entry(437, "IBM437"),
            Map.entry(850, "IBM850"),
            Map.entry(852, "IBM852"),
            Map.entry(866, "IBM866"));

    /** What a code page with no mapping is decoded as: Access 97's default, Western European. */
    public static final Charset FALLBACK = Charset.forName("windows-1252");

    private CodePages() {}

    /** The charset for a code page, or empty when there is no mapping or this Java runtime lacks it. */
    public static Optional<Charset> charset(int codePage) {
        String name = CHARSETS.get(codePage);
        return name != null && Charset.isSupported(name) ? Optional.of(Charset.forName(name)) : Optional.empty();
    }

    static Map<Integer, String> mappings() {
        return CHARSETS;
    }
}
