package io.lytrax.accessconverter.value;

import java.util.List;

/**
 * An attachment, multi-value or version-history cell with its values read (08), for a target that inlines them
 * (JSON). The SQL targets write the values as child tables instead and see only the {@link ComplexRef}.
 *
 * @param items in Access's value-id order: {@link AttachmentValue}s, the multi-value's canonical element values, or
 *     {@link VersionValue}s
 */
public record ComplexValues(int complexId, List<Object> items) {
    public ComplexValues {
        items = java.util.Collections.unmodifiableList(new java.util.ArrayList<>(items));
    }
}
