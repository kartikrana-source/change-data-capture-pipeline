package com.salescode.kafka;


import lombok.extern.slf4j.Slf4j;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.json.JsonDeserializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

/**
 * Factory for creating Kafka sources.
 * Simple utility class with static methods - no instantiation needed.
 */
@Slf4j
public class KafkaSourceProvider {

    // Private constructor - prevents creating objects of this class
    private KafkaSourceProvider() {
    }

    /**
     * Default offset strategy: Resume from checkpoint, if no checkpoint exists start from EARLIEST
     */
    private static final OffsetsInitializer DEFAULT_OFFSETS_INITIALIZER =
            OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST);

    /**
     * Creates KafkaSource with explicit parameters.
     *
     * @param brokers Kafka bootstrap servers (e.g., "localhost:9092" or "broker1:9092,broker2:9092")
     * @param topicPattern Topic name (e.g., "order.changed")
     * @param groupId Consumer group ID for tracking offsets
     * @return Configured KafkaSource
     */
    public static KafkaSource<ObjectNode> createKafkaSource(
            String brokers,
            String topicPattern,
            String groupId) {

        log.info("Setting up Kafka Source:");
        log.info("Brokers: {}", brokers);
        log.info("Topic Pattern: {}", topicPattern);
        log.info("Group ID: {}", groupId);

        // Build and return the KafkaSource
        return KafkaSource.<ObjectNode>builder()
                .setBootstrapServers(brokers)
                .setTopics(topicPattern)
                .setGroupId(groupId)
                .setStartingOffsets(DEFAULT_OFFSETS_INITIALIZER)
                .setValueOnlyDeserializer(new JsonDeserializationSchema<>(ObjectNode.class))
                .build();
    }

    /**
     * Creates KafkaSource reading from properties/config.
     * You'll implement this based on how you read configurations.
     *
     * @return Configured KafkaSource
     */
    public static KafkaSource<ObjectNode> createKafkaSource() {
        // TODO: Read from your config system
        // For now, using placeholder values

        String brokers = "YOUR_KAFKA_BROKERS";  // Replace with actual config reading
        String topicPattern = "YOUR_TOPIC";     // Replace with actual config reading
        String groupId = "YOUR_GROUP_ID";       // Replace with actual config reading

//        log.info("Setting up Kafka Source from configuration:");
//        log.info("Brokers: {}", brokers);
//        log.info("Topic Pattern: {}", topicPattern);
//        log.info("Group ID: {}", groupId);

        return createKafkaSource(brokers, topicPattern, groupId);
    }
}
