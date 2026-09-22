package com.lytrax.accessconverter.report;

import java.util.Locale;

public enum Severity {
    /** Expected behavior worth knowing, e.g. a hidden backing index that was dropped. */
    INFO,
    /** Something was not carried over exactly; the report says what and how often. */
    WARNING,
    /** The conversion failed or data was lost. */
    ERROR;

    public String label() {
        return name().toLowerCase(Locale.ROOT);
    }
}
