package com.salescode.models;


import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.*;

import java.util.List;

/**
 * Configuration for a field with support for nested sub-fields.
 * This is a recursive structure - sub-fields can contain their own sub-fields.
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class FieldConfig {

    /**
     * Path to the field in JSON (e.g., "orderId", "orderDetails")
     */
    @JsonProperty("field-path")
    private String fieldPath;

    /**
     * Data type of the field (e.g., "String", "List", "Object", "Integer", "Double")
     */
    @JsonProperty("type")
    private String type;

    /**
     * Sub-fields for complex types (List or Object).
     * Null for simple types like String, Integer.
     * For List/Object types, contains the nested field structure.
     */
    @JsonProperty("sub-fields")
    private List<FieldConfig> subFields;

    /**
     * Check if this field has sub-fields
     */
    public boolean hasSubFields() {
        return subFields != null && !subFields.isEmpty();
    }

    /**
     * Check if this is a simple field (no sub-fields)
     */
    public boolean isSimpleField() {
        return !hasSubFields();
    }

    /**
     * Check if this is a List type
     */
    public boolean isListType() {
        return "List".equalsIgnoreCase(type);
    }

    /**
     * Check if this is an Object type
     */
    public boolean isMapType() {
        return "Map".equalsIgnoreCase(type);
    }
}
