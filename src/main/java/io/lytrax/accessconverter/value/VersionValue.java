package io.lytrax.accessconverter.value;

import java.time.LocalDateTime;

/**
 * One entry of an append-only memo's version history (08, {@code --include-version-history}).
 *
 * @param id Access's complex value id
 */
public record VersionValue(int id, String value, LocalDateTime modified) {}
