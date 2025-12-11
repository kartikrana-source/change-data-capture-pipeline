package com.salescode;

import com.salescode.config.ConfigReader;
import com.salescode.kafka.KafkaConsumer;
import com.salescode.kafka.KafkaSourceProvider;
import com.salescode.models.EntityConfig;
import com.salescode.models.RootConfig;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterable;
import org.apache.flink.util.CloseableIterator;

import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Main {
    public static void main(String[] args) {
        ConfigReader configReader  = ConfigReader.getInstance();
        RootConfig rootConfig = configReader.loadConfig();
        List<EntityConfig> entityConfigList = rootConfig.getEntities();
        for(EntityConfig config:entityConfigList) {
            KafkaConsumer consumer = new KafkaConsumer(config.getEntityName(),config.getFields());
            StreamExecutionEnvironment environment = consumer.read(config);
            try {
                JobClient client = environment.executeAsync();
                
                long millis = config.getRuntimeMinutes() * 60 * 1000;
                try {
                    var accumulators = client.getAccumulators().get(5, TimeUnit.SECONDS);
                    System.out.println("   Accumulators: " + accumulators);
                } catch (Exception e) {
                    System.out.println("   Accumulators: Not available");
                }
                Thread.sleep(millis);
                try {
                    var accumulators = client.getAccumulators().get(5, TimeUnit.SECONDS);
                    System.out.println("   Accumulators: " + accumulators);
                } catch (Exception e) {
                    System.out.println("   Accumulators: Not available");
                }
                client.cancel().get();  // Cancel and wait

            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }


}