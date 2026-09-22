package com.lytrax.accessconverter.value;

/**
 * An attachment, multi-value or version-history cell: Access's per-row complex id, which keys the hidden table that
 * holds the actual values (08). The values themselves are read by the binary layer.
 */
public record ComplexRef(int complexId) {}
