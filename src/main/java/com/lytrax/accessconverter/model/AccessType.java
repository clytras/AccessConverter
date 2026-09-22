package com.lytrax.accessconverter.model;

/**
 * An Access column type: Jackcess's {@code DataType} names, refined where one storage type means different things
 * to a target (04, Columns).
 */
public enum AccessType {
    BOOLEAN,
    /** Unsigned 0–255 (F-01). */
    BYTE,
    /** Access "Integer": 16-bit. */
    INT,
    /** Access "Long Integer": 32-bit. */
    LONG,
    /** A LONG autonumber. May be negative or random, and needn't be the primary key. */
    AUTONUMBER_LONG,
    /** Access "Large Number": 64-bit. */
    BIG_INT,
    /** Currency: exactly (19,4). */
    MONEY,
    /** Single. */
    FLOAT,
    DOUBLE,
    /** Decimal(precision, scale). */
    NUMERIC,
    SHORT_DATE_TIME,
    /** Date/Time Extended: 100 ns resolution, years 1–9999. */
    EXT_DATE_TIME,
    /** Short Text(n), n in characters. */
    TEXT,
    /** Long Text. */
    MEMO,
    /** A Long Text holding {@code display#address#subaddress#}. */
    HYPERLINK,
    /** Replication ID, stored as {@code {XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX}}. */
    GUID,
    /** A GUID autonumber. */
    AUTONUMBER_GUID,
    /** Binary(n), n in bytes. */
    BINARY,
    OLE,
    ATTACHMENT,
    MULTI_VALUE,
    /** The hidden complex column that holds an append-only memo's history. */
    VERSION_HISTORY,
    /** A complex column of a kind Jackcess doesn't understand. */
    COMPLEX_UNSUPPORTED,
    /** A column type Jackcess can't read; its raw bytes are kept. */
    UNSUPPORTED;

    public boolean isComplex() {
        return switch (this) {
            case ATTACHMENT, MULTI_VALUE, VERSION_HISTORY, COMPLEX_UNSUPPORTED -> true;
            default -> false;
        };
    }

    public boolean isAutoNumber() {
        return this == AUTONUMBER_LONG || this == AUTONUMBER_GUID;
    }

    public boolean isText() {
        return this == TEXT || this == MEMO || this == HYPERLINK;
    }

    public boolean isExactNumeric() {
        return this == MONEY || this == NUMERIC;
    }

    public boolean isDateTime() {
        return this == SHORT_DATE_TIME || this == EXT_DATE_TIME;
    }
}
