package com.salescode.transformer;

import com.salescode.models.FieldConfig;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Transformer to extract and transform fields based on FieldConfig
 */
@Slf4j

public class MainTransformer {

    /**
     * Transform input map to output map based on field configurations.
     * Handles nested fields (List and Map types) recursively.
     *
     * @param configList List of field configurations
     * @param input Input map from Kafka (parsed JSON)
     * @return Transformed output map
     */
    public Map<String, Object> transform(List<FieldConfig> configList, Map<String, Object> input) {
        Map<String, Object> output = new HashMap<>();

        for (FieldConfig fieldConfig : configList) {
            String fieldPath = fieldConfig.getFieldPath();
            Object fieldValue = input.get(fieldPath);

            // Skip if field doesn't exist in input
            if (fieldValue == null) {
                log.debug("Field '{}' not found in input, skipping", fieldPath);
                output.put(fieldPath, null);
                continue;
            }

            // Handle simple fields (no sub-fields)
            if (fieldConfig.isSimpleField()) {
                output.put(fieldPath, fieldValue);
            }
            // Handle List type with sub-fields
            else if (fieldConfig.isListType()) {
                output.put(fieldPath, transformList(fieldConfig, fieldValue));
            }
            // Handle Map/Object type with sub-fields
            else if (fieldConfig.isMapType()) {
                output.put(fieldPath, transformMap(fieldConfig, fieldValue));
            }
            // Fallback for unknown types
            else {
                log.warn("Unknown field type '{}' for field '{}', copying as-is",
                        fieldConfig.getType(), fieldPath);
                output.put(fieldPath, fieldValue);
            }
        }

        return output;
    }

    /**
     * Transform a List field by recursively processing each item with sub-field configurations
     *
     * @param fieldConfig Configuration containing sub-fields
     * @param fieldValue The list value from input
     * @return Transformed list
     */
    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> transformList(FieldConfig fieldConfig, Object fieldValue) {
        // Validate that fieldValue is actually a List
        if (!(fieldValue instanceof List)) {
            log.error("Expected List for field '{}' but got {}",
                    fieldConfig.getFieldPath(), fieldValue.getClass().getSimpleName());
            return Collections.emptyList();
        }

        List<Object> inputList = (List<Object>) fieldValue;
        List<Map<String, Object>> outputList = new ArrayList<>();

        // Process each item in the list
        for (Object item : inputList) {
            if (item instanceof Map) {
                // Recursively transform each list item using sub-field configurations
                Map<String, Object> itemMap = (Map<String, Object>) item;
                Map<String, Object> transformedItem = transform(fieldConfig.getSubFields(), itemMap);
                outputList.add(transformedItem);
            } else {
                log.warn("List item in '{}' is not a Map, skipping transformation",
                        fieldConfig.getFieldPath());
            }
        }

        return outputList;
    }

    /**
     * Transform a Map/Object field by recursively processing with sub-field configurations
     *
     * @param fieldConfig Configuration containing sub-fields
     * @param fieldValue The map value from input
     * @return Transformed map
     */
    @SuppressWarnings("unchecked")
    private Map<String, Object> transformMap(FieldConfig fieldConfig, Object fieldValue) {
        // Validate that fieldValue is actually a Map
        if (!(fieldValue instanceof Map)) {
            log.error("Expected Map for field '{}' but got {}",
                    fieldConfig.getFieldPath(), fieldValue.getClass().getSimpleName());
            return Collections.emptyMap();
        }

        Map<String, Object> inputMap = (Map<String, Object>) fieldValue;

        // Recursively transform using sub-field configurations
        return transform(fieldConfig.getSubFields(), inputMap);
    }

    /**
     * Validate that all required fields exist in input
     *
     * @param configList Field configurations
     * @param input Input map
     * @return List of missing field paths
     */
    public List<String> validateRequiredFields(List<FieldConfig> configList, Map<String, Object> input) {
        List<String> missingFields = new ArrayList<>();

        for (FieldConfig fieldConfig : configList) {
            String fieldPath = fieldConfig.getFieldPath();
            if (!input.containsKey(fieldPath)) {
                missingFields.add(fieldPath);
            }
        }

        return missingFields;
    }

    /**
     * Get all field paths (flattened) from nested configuration
     *
     * @param configList Field configurations
     * @return List of all field paths including nested ones
     */
    public List<String> getAllFieldPaths(List<FieldConfig> configList) {
        return getAllFieldPaths(configList, "");
    }

    /**
     * Helper method to recursively get all field paths
     *
     * @param configList Field configurations
     * @param prefix Current path prefix
     * @return List of field paths
     */
    private List<String> getAllFieldPaths(List<FieldConfig> configList, String prefix) {
        List<String> paths = new ArrayList<>();

        for (FieldConfig fieldConfig : configList) {
            String currentPath = prefix.isEmpty()
                    ? fieldConfig.getFieldPath()
                    : prefix + "." + fieldConfig.getFieldPath();

            paths.add(currentPath);

            // Recursively add sub-field paths
            if (fieldConfig.hasSubFields()) {
                paths.addAll(getAllFieldPaths(fieldConfig.getSubFields(), currentPath));
            }
        }

        return paths;
    }
    /**
     * Test main function to demonstrate transformation
     */
    /**
     * Test main function to demonstrate transformation
     */
    public static void main(String[] args) {
        System.out.println("========================================");
        System.out.println("FIELD TRANSFORMER TEST");
        System.out.println("========================================\n");

        MainTransformer transformer = new MainTransformer();

        // ==================== CREATE INPUT ====================
        Map<String, Object> input = new HashMap<>();

        // Simple String fields
        input.put("orderId", "ORD-12345");
        input.put("outletCode", "OUT-001");
        input.put("creationTime", "2025-01-08T10:30:00Z");
        input.put("status", "CONFIRMED");

        // Extra fields that should be FILTERED OUT
        input.put("extraField1", "This should be ignored");
        input.put("extraField2", "This too should be ignored");

        // List field with sub-fields (orderDetails)
        List<Map<String, Object>> orderDetailsList = new ArrayList<>();

        Map<String, Object> item1 = new HashMap<>();
        item1.put("skuCode", "SKU-001");
        item1.put("initialAmount", "100.50");
        item1.put("initialQuantity", "5");
        item1.put("discount", "10.00");  // Extra field - should be filtered
        item1.put("tax", "18.00");       // Extra field - should be filtered
        orderDetailsList.add(item1);

        Map<String, Object> item2 = new HashMap<>();
        item2.put("skuCode", "SKU-002");
        item2.put("initialAmount", "250.75");
        item2.put("initialQuantity", "10");
        item2.put("discount", "25.00");  // Extra field - should be filtered
        item2.put("merchantId", "M123"); // Extra field - should be filtered
        orderDetailsList.add(item2);

        input.put("orderDetails", orderDetailsList);

        // Map field with sub-fields (customerInfo)
        Map<String, Object> customerInfo = new HashMap<>();
        customerInfo.put("customerId", "CUST-789");
        customerInfo.put("customerName", "John Doe");
        customerInfo.put("phoneNumber", "+91-9876543210");
        customerInfo.put("creditScore", "750");      // Extra field - should be filtered
        customerInfo.put("membershipLevel", "GOLD"); // Extra field - should be filtered
        input.put("customerInfo", customerInfo);

        // Another extra field at root level
        input.put("paymentMethod", "CARD"); // Should be filtered

        // ==================== CREATE FIELD CONFIGS ====================
        List<FieldConfig> fieldConfigs = new ArrayList<>();

        // Simple fields
        fieldConfigs.add(new FieldConfig("orderId", "String", null));
        fieldConfigs.add(new FieldConfig("outletCode", "String", null));
        fieldConfigs.add(new FieldConfig("creationTime", "String", null));
        fieldConfigs.add(new FieldConfig("status", "String", null));

        // List field with sub-fields
        List<FieldConfig> orderDetailsSubFields = new ArrayList<>();
        orderDetailsSubFields.add(new FieldConfig("skuCode", "String", null));
        orderDetailsSubFields.add(new FieldConfig("initialAmount", "String", null));
        orderDetailsSubFields.add(new FieldConfig("initialQuantity", "String", null));

        fieldConfigs.add(new FieldConfig("orderDetails", "List", orderDetailsSubFields));

        // Map field with sub-fields
        List<FieldConfig> customerInfoSubFields = new ArrayList<>();
        customerInfoSubFields.add(new FieldConfig("customerId", "String", null));
        customerInfoSubFields.add(new FieldConfig("customerName", "String", null));
        customerInfoSubFields.add(new FieldConfig("phoneNumber", "String", null));

        fieldConfigs.add(new FieldConfig("customerInfo", "Map", customerInfoSubFields));

        // ==================== PRINT INPUT ====================
        System.out.println("\n========== INPUT (FROM KAFKA) ==========");
        printMap(input, 0);

        System.out.println("\n========== FIELD CONFIGURATIONS ==========");
        printFieldConfigs(fieldConfigs, 0);

        // ==================== TRANSFORM ====================
        System.out.println("\n========== TRANSFORMING... ==========");
        Map<String, Object> output = transformer.transform(fieldConfigs, input);

        // ==================== PRINT OUTPUT ====================
        System.out.println("\n========== OUTPUT (TRANSFORMED) ==========");
        printMap(output, 0);

        // ==================== VALIDATION ====================
        System.out.println("\n========== VALIDATION ==========");
        List<String> missingFields = transformer.validateRequiredFields(fieldConfigs, input);
        if (missingFields.isEmpty()) {
            System.out.println("✓ All required fields present in input");
        } else {
            System.out.println("✗ Missing fields: " + missingFields);
        }

        System.out.println("\n========== ALL FIELD PATHS (FLATTENED) ==========");
        List<String> allPaths = transformer.getAllFieldPaths(fieldConfigs);
        allPaths.forEach(path -> System.out.println("  - " + path));

        System.out.println("\n========================================");
        System.out.println("TEST COMPLETED");
        System.out.println("========================================");
    }

    /**
     * Helper method to print Map recursively
     */
    private static void printMap(Map<String, Object> map, int indent) {
        String indentStr = "  ".repeat(indent);

        for (Map.Entry<String, Object> entry : map.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();

            if (value instanceof Map) {
                System.out.println(indentStr + key + ":");
                printMap((Map<String, Object>) value, indent + 1);
            } else if (value instanceof List) {
                System.out.println(indentStr + key + ": [");
                List<?> list = (List<?>) value;
                for (int i = 0; i < list.size(); i++) {
                    Object item = list.get(i);
                    if (item instanceof Map) {
                        System.out.println(indentStr + "  Item " + (i + 1) + ":");
                        printMap((Map<String, Object>) item, indent + 2);
                    } else {
                        System.out.println(indentStr + "  " + item);
                    }
                }
                System.out.println(indentStr + "]");
            } else {
                System.out.println(indentStr + key + ": " + value);
            }
        }
    }

    /**
     * Helper method to print FieldConfig recursively
     */
    private static void printFieldConfigs(List<FieldConfig> configs, int indent) {
        String indentStr = "  ".repeat(indent);

        for (FieldConfig config : configs) {
            System.out.println(indentStr + "- field-path: " + config.getFieldPath());
            System.out.println(indentStr + "  type: " + config.getType());

            if (config.hasSubFields()) {
                System.out.println(indentStr + "  sub-fields:");
                printFieldConfigs(config.getSubFields(), indent + 2);
            }
        }
    }
}