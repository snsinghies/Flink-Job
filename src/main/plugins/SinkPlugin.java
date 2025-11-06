package org.example.flink.plugins;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.example.flink.event.DataRecord;

public interface SinkPlugin extends Plugin {
    void addSink(DataStream<DataRecord> stream);
}
