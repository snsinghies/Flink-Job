# Plugin-Based Flink JOB SUBMITTING along with KAFKA and MYSQL

This project demonstrates a **plugin-based architecture** for Apache Flink, enabling flexible ETL pipelines that can be configured to use different sources, processors, and sinks—all via configuration, with no code changes required.

## 🚀 Features

- **No Code Changes Needed:** Switch between sources (MongoDB, Kafka, etc.) and sinks (Kafka, Doris, Console, File, etc.) by editing a YAML config file.
- **Plugin Architecture:** Easily extend the pipeline with new source, processor, or sink plugins.
- **Configuration-Driven:** All pipeline logic is controlled by `config/pipeline.yaml`.
- **Mix & Match:** Combine any supported source, processor, and sink plugins.
- **Integration Tests:** Includes integration tests for plugin-based pipelines.

## 📁 Project Structure

```
flink-plugin-poc/
├── docker-compose.yml
├── pom.xml
├── README.md
├── requirements.txt
├── config/
│   └── pipeline.yaml
├── scripts/
│   ├── flink.sh
│   └── send_events.py
├── src/
│   └── main/java/org/example/flink/
│       ├── PluginBasedFlinkApp.java
│       ├── beans/
│       │   └── PipelineConfiguration.java
│       ├── event/
│       │   ├── AddressEntity.java
│       │   ├── DataRecord.java
│       │   └── UserEntity.java
│       ├── plugins/
│       │   ├── Plugin.java
│       │   ├── PluginConfig.java
│       │   ├── ProcessorPlugin.java
│       │   ├── SinkPlugin.java
│       │   ├── SourcePlugin.java
│       │   ├── processors/
│       │   ├── sinks/
│       │   └── sources/
│       └── util/
│           └── PipelineConfigReader.java
│   └── resources/
├── test/
│   └── java/org/example/flink/
│       ├── PluginBasedFlinkAppIntegrationTest.java
│       └── plugins/
│           ├── sinks/
│           └── sources/
└── target/
    ├── flink-plugin-poc-1.0.0.jar
    └── ...
```

## ⚙️ Configuration Example (`config/pipeline.yaml`)

```yaml
pipeline:
  name: "flexible-etl-pipeline"
  parallelism: 2
  checkpointing:
    enabled: true
    interval: 5000

source:
  type: "kafka"
  config:
    kafka:
      bootstrap_servers: "kafka:29092"
      topics: ["input-orders"]
      group_id: "flink-consumer-group"

processors:
  - type: "json_parser"
    enabled: true
    config:
      fields_to_extract: ["order_id", "customer_id", "product_id", "quantity", "price", "status"]
  - type: "timestamp_enricher"
    enabled: true
    config:
      timestamp_field: "processed_time"
      format: "ISO_LOCAL_DATE_TIME"
  - type: "data_validator"
    enabled: true
    config:
      rules:
        - field: "order_id"
          required: true
          min_length: 3
        - field: "price"
          required: true
          min_value: 0

sinks:
  - type: "kafka"
    enabled: true
    config:
      bootstrap_servers: "kafka:29092"
      topic: "processed-orders"
```



## Prerequisites
- Docker and Docker Compose installed
- Java 11 or higher
- Apache Maven installed
- Python 3.x (for event generation script)

## Hardware Requirements
- Minimum 8 GB RAM
- At least 4 CPU cores
- 50 GB disk space


## 🏁 Quick Start
1. **Start Services**
   ```bash
   ./execute.sh start
   ```
2. **Stop Services**
   ```bash
   ./execute.sh stop
   ```
3. **Open in Browser Using Below Link** 
   ```
   http://localhost:8081/#/overview
   ```
4. **Test With Sample Data**
   ```bash
   ./execute.sh test
   ```

## 🔧 Adding New Plugins

1. **Create Plugin Class:** Implement `SourcePlugin`, `ProcessorPlugin`, or `SinkPlugin` in `src/main/java/org/example/flink/plugins/`.
2. **Register Plugin:** Add registration logic in `PluginBasedFlinkApp.java`.
3. **Configure in YAML:** Reference your plugin by type in `pipeline.yaml`.

Example:
```java
public class CustomProcessorPlugin implements ProcessorPlugin {
    @Override
    public String getType() { return "custom_processor"; }
    @Override
    public DataStream<DataRecord> process(DataStream<DataRecord> input) {
        // Custom logic
    }
}
```

## 📚 References
- [Apache Flink Documentation](https://nightlies.apache.org/flink/)

---
