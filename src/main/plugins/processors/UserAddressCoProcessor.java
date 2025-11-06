package org.example.flink.plugins.processors;

import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.example.flink.event.AddressEntity;
import org.example.flink.event.DataRecord;
import org.example.flink.event.UserEntity;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class UserAddressCoProcessor extends KeyedCoProcessFunction<String, DataRecord, DataRecord, DataRecord> {
    private final int timeoutSeconds;
    private final int stateRetentionMinutes;
    private final boolean enablePartialEnrichment;

    // State to store user and addresses
    private transient ValueState<UserEntity> userState;
    private transient ListState<AddressEntity> addressesState;
    private transient ValueState<Long> timerState;

    public UserAddressCoProcessor(int timeoutSeconds, int stateRetentionMinutes, boolean enablePartialEnrichment) {
        this.timeoutSeconds = timeoutSeconds;
        this.stateRetentionMinutes = stateRetentionMinutes;
        this.enablePartialEnrichment = enablePartialEnrichment;
    }

    @Override
    public void processElement2(DataRecord addressRecord, Context context, Collector<DataRecord> collector) throws Exception {
        // Process address event
        AddressEntity address = AddressEntity.fromDataRecord(addressRecord);
        addressesState.add(address);
        System.out.println("Received address: " + address.getAddressId() + " for user: " + address.getUserId());
        // Emit enriched user event on address event
        tryEnrichAndEmit(collector, context.timerService().currentProcessingTime());
    }

    @Override
    public void processElement1(DataRecord userRecord, Context context, Collector<DataRecord> collector) throws Exception {
        // Process user event
        UserEntity user = UserEntity.fromDataRecord(userRecord);
        userState.update(user);
        System.out.println("Received user: " + user.getUserId());
        // Emit enriched user event on user event
        tryEnrichAndEmit(collector, context.timerService().currentProcessingTime());
        // Optionally set timer for partial enrichment if needed
        if (enablePartialEnrichment) {
            long timer = context.timerService().currentProcessingTime() + (timeoutSeconds * 1000L);
            context.timerService().registerProcessingTimeTimer(timer);
            timerState.update(timer);
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<DataRecord> out) throws Exception {
        // Timer fired - emit enriched user with all addresses collected so far
        System.out.println("Timer fired for user: " + ctx.getCurrentKey());
        tryEnrichAndEmit(out, ctx.timerService().currentProcessingTime());
        // Optionally clear timerState (not user/address state)
        timerState.clear();
    }

    private void tryEnrichAndEmit(Collector<DataRecord> out, Long timestamp) throws Exception {
        UserEntity user = userState.value();
        if (user == null) {
            return; // No user available yet
        }
        List<AddressEntity> addresses = new ArrayList<>();
        if (addressesState.get() != null) {
            for (AddressEntity address : addressesState.get()) {
                addresses.add(address);
            }
        }
        DataRecord enrichedUser = createEnrichedUserRecord(user, addresses, timestamp);
        out.collect(enrichedUser);
        System.out.println("Emitted enriched user: " + user.getUserId() + " with " + addresses.size() + " addresses");
    }

    private DataRecord createEnrichedUserRecord(UserEntity user, List<AddressEntity> addresses, long timestamp) {
        Map<String, Object> enrichedFields = new HashMap<>(user.getFields());

        // Add addresses as a list
        List<Map<String, Object>> addressList = new ArrayList<>();
        for (AddressEntity address : addresses) {
            addressList.add(address.getFields());
        }
        enrichedFields.put("addresses", addressList);
        enrichedFields.put("address_count", addresses.size());
        enrichedFields.put("enriched_at", LocalDateTime.now().toString());
        enrichedFields.put("enrichment_type", "user_with_addresses");

        DataRecord enrichedRecord = new DataRecord(enrichedFields);
        enrichedRecord.setSourceType("enriched");
        enrichedRecord.setTimestamp(timestamp);
        enrichedRecord.getMetadata().put("processed_by", "user_address_enrichment");
        enrichedRecord.getMetadata().put("original_user_id", user.getUserId());

        return enrichedRecord;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        // Configure state with TTL
        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Time.minutes(stateRetentionMinutes))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();

        // User state
        ValueStateDescriptor<UserEntity> userDescriptor = new ValueStateDescriptor<>(
                "user-state", TypeInformation.of(UserEntity.class));
        userDescriptor.enableTimeToLive(ttlConfig);
        userState = getRuntimeContext().getState(userDescriptor);

        // Addresses state
        ListStateDescriptor<AddressEntity> addressDescriptor = new ListStateDescriptor<>(
                "addresses-state", TypeInformation.of(AddressEntity.class));
        addressDescriptor.enableTimeToLive(ttlConfig);
        addressesState = getRuntimeContext().getListState(addressDescriptor);

        // Timer state
        ValueStateDescriptor<Long> timerDescriptor = new ValueStateDescriptor<>(
                "timer-state", Long.class);
        timerState = getRuntimeContext().getState(timerDescriptor);
    }
}
