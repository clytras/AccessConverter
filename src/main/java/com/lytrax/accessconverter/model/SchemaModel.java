package com.lytrax.accessconverter.model;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Everything extraction knows about an Access database, independent of any target.
 *
 * @param tables local and linked user tables, ordered by name ignoring case
 * @param relationships ordered by name ignoring case
 */
public record SchemaModel(Source source, List<TableModel> tables, List<ForeignKeyModel> relationships) {

    public SchemaModel {
        Objects.requireNonNull(source, "source");
        tables = List.copyOf(tables);
        relationships = List.copyOf(relationships);
    }

    public Optional<TableModel> table(String name) {
        return tables.stream().filter(t -> t.name().equalsIgnoreCase(name)).findFirst();
    }

    /** @param fileFormat Jackcess's name for the file format, e.g. {@code V2010} */
    public record Source(String fileName, String fileFormat) {}
}
