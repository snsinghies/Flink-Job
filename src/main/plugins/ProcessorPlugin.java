package org.example.flink.plugins;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.example.flink.event.DataRecord;

public interface ProcessorPlugin extends Plugin {
    DataStream<DataRecord> process(DataStream<DataRecord> input);
}
