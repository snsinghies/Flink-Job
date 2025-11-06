package org.example.flink.plugins.processors;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.example.flink.event.DataRecord;
import org.example.flink.plugins.PluginConfig;
import org.example.flink.plugins.ProcessorPlugin;

import java.util.Map;

public class LatestUserDeduplicationProcessor implements ProcessorPlugin {
    private PluginConfig config;
    private boolean configured = false;

    @Override
    public DataStream<DataRecord> process(DataStream<DataRecord> input) {
        if (!configured) {
            throw new IllegalStateException("Deduplication processor not configured");
        }

        int timeoutMinutes = config.getInt("timeout_minutes", 5);

        // Key by user_id
        KeyedStream<DataRecord, String> keyed = input.keyBy(record -> record.getFieldAsString("user_id"));
        return keyed.process(new KeyedProcessFunction<>() {
            private transient ValueState<DataRecord> latestState;
            private transient ValueState<Long> timerState;
            private final long DEDUP_TIMEOUT_MS = (long) timeoutMinutes * 60 * 1000;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<DataRecord> desc = new ValueStateDescriptor<>(
                        "latest-user-record", TypeInformation.of(DataRecord.class));
                latestState = getRuntimeContext().getState(desc);
                ValueStateDescriptor<Long> timerDesc = new ValueStateDescriptor<>(
                        "dedup-timer", Long.class);
                timerState = getRuntimeContext().getState(timerDesc);
            }

            @Override
            public void processElement(DataRecord value, Context ctx, Collector<DataRecord> out) throws Exception {
                // Update state with latest event
                latestState.update(value);
                // Cancel previous timer if exists
                Long previousTimer = timerState.value();
                if (previousTimer != null) {
                    ctx.timerService().deleteProcessingTimeTimer(previousTimer);
                }
                // Register new timer for 5 minutes from now
                long timer = ctx.timerService().currentProcessingTime() + DEDUP_TIMEOUT_MS;
                ctx.timerService().registerProcessingTimeTimer(timer);
                timerState.update(timer);
                // Do NOT emit immediately
            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<DataRecord> out) throws Exception {
                DataRecord latest = latestState.value();
                if (latest != null) {
                    out.collect(latest);
                }
                // Clear timer state (but keep latestState for future events)
                timerState.clear();
            }
        });
    }

    @Override
    public String getType() {
        return "deduplication-processor";
    }

    @Override
    public String getDescription() {
        return "Event Deduplication Processor that retains only the latest event for each user";
    }

    @Override
    public void configure(Map<String, Object> config) {
        this.config = new PluginConfig(config);
        this.configured = true;
        System.out.println("Latest User Deduplication Processor configured: " + config);
    }

    @Override
    public boolean isConfigured() {
        return configured;
    }
}
