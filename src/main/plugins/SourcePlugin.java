package org.example.flink.plugins;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.flink.event.DataRecord;

public interface SourcePlugin extends Plugin {
    DataStream<DataRecord> createSource(StreamExecutionEnvironment env);
}
