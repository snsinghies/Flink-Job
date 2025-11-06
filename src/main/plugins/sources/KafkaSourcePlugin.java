package org.example.flink.plugins.sources;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.flink.event.DataRecord;
import org.example.flink.plugins.PluginConfig;
import org.example.flink.plugins.SourcePlugin;

import java.util.List;
import java.util.Map;

public class KafkaSourcePlugin implements SourcePlugin {
    private PluginConfig config;
    private boolean configured = false;

    @Override
    public String getType() {
        return "kafka";
    }

    @Override
    public String getDescription() {
        return "Kafka Source for streaming data";
    }

    @Override
    public void configure(Map<String, Object> config) {
        this.config = new PluginConfig(config);
        this.configured = true;
        System.out.println("Kafka Source Plugin configured: " + config);
    }

    @Override
    public boolean isConfigured() {
        return configured;
    }

    @Override
    public DataStream<DataRecord> createSource(StreamExecutionEnvironment env) {
        if (!configured) {
            throw new IllegalStateException("Kafka source plugin not configured");
        }

        String bootstrapServers = config.getString("bootstrap_servers");
        List<String> topics = config.getStringList("topics");
        String groupId = config.getString("group_id", "flink-consumer");

        System.out.println("Creating Kafka source: " + bootstrapServers + ", topics: " + topics);

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(topics.toArray(new String[0]))
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        var source = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source");
        return source.map(new KafkaRecordMapper()).name("Kafka Record Mapper");
    }

    private static class KafkaRecordMapper implements MapFunction<String, DataRecord> {
        @Override
        public DataRecord map(String message) throws Exception {
            System.out.println("DEBUG: KafkaRecordMapper received message: " + message);
            DataRecord record = new DataRecord();
            record.setSourceType("kafka");

            Map<String, Object> fields = parseJson(message);
            record.setFields(fields);
            record.getMetadata().put("raw_message", message);

            System.out.println("DEBUG: KafkaRecordMapper parsed fields: " + fields);
            return record;
        }

        private Map<String, Object> parseJson(String json) {
            Map<String, Object> fields = new java.util.HashMap<>();

            if (json.startsWith("{") && json.endsWith("}")) {
                extractField(json, "entity_type", fields);
                extractField(json, "user_id", fields);
                extractField(json, "address_id", fields);
                extractField(json, "email", fields);
                extractField(json, "name", fields);
                extractField(json, "street", fields);
                extractField(json, "city", fields);
                extractField(json, "timestamp", fields);
            } else {
                fields.put("message", json);
            }

            return fields;
        }

        private void extractField(String json, String fieldName, Map<String, Object> fields) {
            try {
                String pattern = "\"" + fieldName + "\"\\s*:\\s*\"?([^,}\"]+)\"?";
                java.util.regex.Pattern p = java.util.regex.Pattern.compile(pattern);
                java.util.regex.Matcher m = p.matcher(json);
                if (m.find()) {
                    String value = m.group(1).trim();
                    fields.put(fieldName, value);
                }
            } catch (Exception e) {
                System.err.println("Error extracting field " + fieldName + ": " + e.getMessage());
            }
        }
    }
}
