package com.salescode.models;


import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;
import java.util.List;

/**
 * Configuration for a single entity (Order, Customer, Product, etc.)
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class EntityConfig {

    /**
     * Entity name (e.g., "Order", "Customer")
     */
    @JsonProperty("entity-name")
    private String entityName;

    /**
     * List of LOBs (Line of Business) this entity belongs to
     * e.g., ["dabur", "hul", "marico"]
     */
    @JsonProperty("lobs")
    private List<String> lobs;

    /**
     * Kafka topic to read from (e.g., "order.changed")
     */
    @JsonProperty("kafka-topic")
    private String kafkaTopic;

    /**
     * Kafka Broker to connect to (e.g., "order.changed")
     */
    @JsonProperty("kafka-broker")
    private String kafkaBroker;

    /**
     * Group Id read from (e.g., "order.changed")
     */
    @JsonProperty("group-id")
    private String groupId;

    /**
     * Iceberg table name (e.g., "orders")
     */
    @JsonProperty("iceberg-table")
    private String icebergTable;

    /**
     * Output class name (e.g., "OrderIcebergEntity")
     */
    @JsonProperty("output-class")
    private String outputClass;


    /**
     * Minutes max allotted for this job (e.g., "OrderIcebergEntity")
     */
    @JsonProperty("runtime-minutes")
    private Integer runtimeMinutes;

    /**
     * List of fields with their types and optional sub-fields
     */
    @JsonProperty("fields")
    private List<FieldConfig> fields;

    /**
     * Partition keys for Iceberg table
     */
    @JsonProperty("partition-keys")
    private List<String> partitionKeys;

    /**
     * Whether this entity pipeline is enabled
     */
    @JsonProperty("enabled")
    private boolean enabled = true;
}
