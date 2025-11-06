package org.example.flink.plugins.processors;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.example.flink.event.DataRecord;
import org.example.flink.plugins.PluginConfig;
import org.example.flink.plugins.ProcessorPlugin;

import java.util.Map;

public class UserAddressEnrichmentProcessor implements ProcessorPlugin {
    private PluginConfig config;
    private boolean configured = false;

    @Override
    public DataStream<DataRecord> process(DataStream<DataRecord> input) {
        if (!configured) {
            throw new IllegalStateException("User-Address enrichment processor not configured");
        }

        // Configuration
        int timeoutSeconds = config.getInt("timeout_seconds", 30);
        int stateRetentionMinutes = config.getInt("state_retention_minutes", 60);
        boolean enablePartialEnrichment = config.getBoolean("enable_partial_enrichment", true);

        // Step 1: Route events by entity type
        OutputTag<DataRecord> addressTag = new OutputTag<>("addresses") {};

        SingleOutputStreamOperator<DataRecord> userStream = input.process(
                new ProcessFunction<DataRecord, DataRecord>() {
                    @Override
                    public void processElement(DataRecord record, Context ctx, Collector<DataRecord> out) {
                        String entityType = record.getFieldAsString("entity_type");
                        System.out.println("DEBUG: Processing record with entity_type: " + entityType + ", fields: " + record.getFields());

                        // Infer entity type if not explicitly set
                        if (entityType == null) {
                            if (record.getField("email") != null || record.getField("name") != null) {
                                entityType = "user";
                                System.out.println("DEBUG: Inferred entity_type as 'user' from email/name fields");
                            } else if (record.getField("street") != null || record.getField("address_id") != null) {
                                entityType = "address";
                                System.out.println("DEBUG: Inferred entity_type as 'address' from street/address_id fields");
                            }
                        }

                        if ("user".equals(entityType)) {
                            System.out.println("DEBUG: Routing as user record: " + record.getFieldAsString("user_id"));
                            out.collect(record);
                        } else if ("address".equals(entityType)) {
                            System.out.println("DEBUG: Routing as address record: " + record.getFieldAsString("address_id") + " for user: " + record.getFieldAsString("user_id"));
                            ctx.output(addressTag, record);
                        } else {
                            System.out.println("Unknown entity type: " + entityType + " in record: " + record);
                        }
                    }
                }
        ).name("Entity Router");

        DataStream<DataRecord> addressStream = userStream.getSideOutput(addressTag);

        // Step 2: Key both streams by user_id
        KeyedStream<DataRecord, String> keyedUsers = userStream.keyBy(record -> {
            String userId = record.getFieldAsString("user_id");
            System.out.println("DEBUG: Keying user record by user_id: " + userId);
            return userId;
        });

        KeyedStream<DataRecord, String> keyedAddresses = addressStream.keyBy(record -> {
            String userId = record.getFieldAsString("user_id");
            System.out.println("DEBUG: Keying address record by user_id: " + userId);
            return userId;
        });

        // Step 3: Co-process to enrich users with addresses
        return keyedUsers
                .connect(keyedAddresses)
                .process(new UserAddressCoProcessor(timeoutSeconds, stateRetentionMinutes, enablePartialEnrichment))
                .name("User-Address Enrichment");
    }

    @Override
    public String getType() {
        return "user_address_enrichment";
    }

    @Override
    public String getDescription() {
        return "Enriches User entities with Address data from the same stream";
    }

    @Override
    public void configure(Map<String, Object> config) {
        this.config = new PluginConfig(config);
        this.configured = true;
        System.out.println("User-Address Enrichment Processor configured");
    }

    @Override
    public boolean isConfigured() {
        return configured;
    }
}
