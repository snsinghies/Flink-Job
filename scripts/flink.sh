#!/bin/bash
flink run -m flink-jobmanager:8081 \
 -c org.example.flink.PluginBasedFlinkApp \
 /opt/flink/usrlib/flink-plugin-mysql-to-kafka-1.0.0.jar
