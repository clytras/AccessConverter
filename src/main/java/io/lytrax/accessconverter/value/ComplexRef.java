package io.lytrax.accessconverter.value;

/**
 * An attachment, multi-value or version-history cell: Access's per-row complex id, which keys the hidden table that
 * holds the actual values (08). The SQL targets write those values as child tables keyed by this id; JSON reads them
 * inline ({@link ComplexValues}).
 */
public record ComplexRef(int complexId) {}
