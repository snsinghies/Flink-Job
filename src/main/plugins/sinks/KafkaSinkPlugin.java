package org.example.flink.plugins.sinks;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.example.flink.event.DataRecord;
import org.example.flink.plugins.PluginConfig;
import org.example.flink.plugins.SinkPlugin;

import java.util.Map;
import java.util.Properties;

public class KafkaSinkPlugin implements SinkPlugin {
    private PluginConfig config;
    private boolean configured = false;

    @Override
    public String getType() {
        return "kafka";
    }

    @Override
    public String getDescription() {
        return "Kafka Sink for streaming output";
    }

    @Override
    public void configure(Map<String, Object> config) {
        this.config = new PluginConfig(config);
        this.configured = true;
        System.out.println("Kafka Sink Plugin configured: " + config);
    }

    @Override
    public boolean isConfigured() {
        return configured;
    }

    @Override
    public void addSink(DataStream<DataRecord> stream) {
        if (!configured) {
            throw new IllegalStateException("Kafka sink plugin not configured");
        }

        String bootstrapServers = config.getString("bootstrap_servers");
        String topic = config.getString("topic");

        DataStream<String> jsonStream = stream.map(new DataRecordToJsonMapper())
                .name("DataRecord to JSON");

        Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty("bootstrap.servers", bootstrapServers);
        kafkaProperties.setProperty("transaction.timeout.ms", "3600000");

        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic(topic)
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .setKafkaProducerConfig(kafkaProperties)
                .build();

        jsonStream.sinkTo(kafkaSink).name("Kafka Sink");
    }

    private static class DataRecordToJsonMapper implements MapFunction<DataRecord, String> {
        @Override
        public String map(DataRecord record) {
            System.out.println("DEBUG: Mapping DataRecord with sourceType: " + record.getSourceType());
            StringBuilder json = new StringBuilder("{");
            
            boolean first = true;
            
            // Add source_type first
            if (record.getSourceType() != null) {
                json.append("\"source_type\":\"").append(record.getSourceType()).append("\"");
                first = false;
                System.out.println("DEBUG: Added source_type to JSON");
            } else {
                System.out.println("DEBUG: sourceType is null!");
            }
            
            // Add all fields
            for (Map.Entry<String, Object> entry : record.getFields().entrySet()) {
                if (!first) json.append(",");
                json.append("\"").append(entry.getKey()).append("\":");
                
                Object value = entry.getValue();
                if (value instanceof String) {
                    json.append("\"").append(value).append("\"");
                } else if (value instanceof Number) {
                    json.append(value);
                } else if (value instanceof Boolean) {
                    json.append(value);
                } else if (value == null) {
                    json.append("null");
                } else {
                    json.append("\"").append(value).append("\"");
                }
                first = false;
            }
            
            // Add metadata if present
            if (record.getMetadata() != null && !record.getMetadata().isEmpty()) {
                for (Map.Entry<String, Object> entry : record.getMetadata().entrySet()) {
                    if (!first) json.append(",");
                    json.append("\"").append(entry.getKey()).append("\":");
                    
                    Object value = entry.getValue();
                    if (value instanceof String) {
                        json.append("\"").append(value).append("\"");
                    } else if (value instanceof Number) {
                        json.append(value);
                    } else if (value instanceof Boolean) {
                        json.append(value);
                    } else if (value == null) {
                        json.append("null");
                    } else {
                        json.append("\"").append(value).append("\"");
                    }
                    first = false;
                }
            }
            
            json.append("}");
            String result = json.toString();
            System.out.println("DEBUG: Final JSON output: " + result);
            return result;
        }
    }
}
