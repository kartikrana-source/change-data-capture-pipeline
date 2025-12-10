package com.salescode.kafka;

import com.salescode.models.EntityConfig;
import com.salescode.models.FieldConfig;
import com.salescode.sink.IcebergSink;
import com.salescode.transformer.MainTransformer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class KafkaConsumer {

    // ✅ Create ObjectMapper once (reuse for all messages)
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    // ✅ Instance variables with proper initialization
    private final IcebergSink sink;
    private final MainTransformer mainTransformer;
    private final List<FieldConfig> fieldConfigs;

    /**
     * Constructor - loads field configs from YAML
     */
    public KafkaConsumer(String entityName,List<FieldConfig> fieldConfig) {
        this.sink = new IcebergSink(100);  // ✅ Batch size 100
        this.mainTransformer = new MainTransformer();
        this.fieldConfigs = fieldConfig;

    }

    public StreamExecutionEnvironment read(EntityConfig config) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(300000);
        DataStream<ObjectNode> kafkaStream = env.fromSource(
                KafkaSourceProvider.createKafkaSource(config.getKafkaBroker(),config.getKafkaTopic(),config.getGroupId()),
                WatermarkStrategy.noWatermarks(),
                "Kafka Source"
        );

        DataStream<Map<String, Object>> processedStream = kafkaStream
                .map(objectNode ->
                    (Map<String,Object>) OBJECT_MAPPER.convertValue(objectNode, Map.class)

                )
                .map(map ->
                    // ✅ Use actual field configs (not empty ArrayList)
                     mainTransformer.transform(fieldConfigs, map)
                )
                .name("ObjectNode to Map → Transform");

        processedStream.addSink(sink)
                .name("Iceberg Sink");

        return env;
    }
}