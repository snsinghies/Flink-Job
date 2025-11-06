package org.example.flink;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.flink.beans.PipelineConfiguration;
import org.example.flink.event.DataRecord;
import org.example.flink.plugins.ProcessorPlugin;
import org.example.flink.plugins.SinkPlugin;
import org.example.flink.plugins.SourcePlugin;
import org.example.flink.plugins.processors.LatestUserDeduplicationProcessor;
import org.example.flink.plugins.processors.UserAddressEnrichmentProcessor;
import org.example.flink.plugins.sinks.KafkaSinkPlugin;
import org.example.flink.plugins.sources.KafkaSourcePlugin;
import org.example.flink.util.PipelineConfigReader;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class PluginExecutor {
    private static volatile PluginExecutor executor;

    private final ParameterTool params;
    private final PipelineConfiguration config;
    private final Map<String, SourcePlugin> sourcePlugins = new HashMap<>();
    private final Map<String, ProcessorPlugin> processorPlugins = new HashMap<>();
    private final Map<String, SinkPlugin> sinkPlugins = new HashMap<>();

    private PluginExecutor(String[] args) {
        params = ParameterTool.fromArgs(args);
        config = loadConfig();
        register();
    }

    public static synchronized PluginExecutor getInstance(String[] args) {
        if(executor == null) {
            executor = new PluginExecutor(args);
        }
        return executor;
    }

    public void execute() throws Exception {
        // Setup Flink environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);
        configure(env);

        // Create source
        DataStream<DataRecord> sourceStream = createSource(env);

        // Apply processors
        DataStream<DataRecord> processedStream = applyProcessors(sourceStream);
 
        // Add sinks
        addSinks(processedStream);

        // Execute pipeline
        String pipelineName = config.getPipeline().getName();
        System.out.println("Starting pipeline: " + pipelineName);
        env.execute(pipelineName);
    }

    private DataStream<DataRecord> createSource(StreamExecutionEnvironment env) {
        PipelineConfiguration.Source sourceConfig = config.getSource();
        String sourceType = sourceConfig.getType();
        System.out.println("Creating source: " + sourceType);
        SourcePlugin sourcePlugin = sourcePlugins.get(sourceType);
        if (sourcePlugin == null) {
            throw new IllegalArgumentException("Unknown source type: " + sourceType);
        }

        // Configure plugin
        Map<String, Object> pluginConfig = sourceConfig.getConfig();
        sourcePlugin.configure((Map<String, Object>) pluginConfig.get(sourceType));
        return sourcePlugin.createSource(env);
    }

    private DataStream<DataRecord> applyProcessors(DataStream<DataRecord> stream) {
        List<PipelineConfiguration.Processor> processors = config.getProcessors();

        if (processors == null || processors.isEmpty()) {
            System.out.println("No processors configured");
            return stream;
        }

        DataStream<DataRecord> currentStream = stream;
        for (PipelineConfiguration.Processor processorConfig : processors) {
            String processorType = processorConfig.getType();
            Boolean enabled = processorConfig.getEnabled();

            if (enabled == null || !enabled) {
                System.out.println("Skipping disabled processor: " + processorType);
                continue;
            }

            System.out.println("Applying processor: " + processorType);

            ProcessorPlugin processor = processorPlugins.get(processorType);
            if (processor == null) {
                System.err.println("Unknown processor type: " + processorType);
                continue;
            }

            // Configure processor
            Map<String, Object> pluginConfig = processorConfig.getConfig();
            processor.configure(pluginConfig);
            // Apply processor
            currentStream = processor.process(currentStream);
        }

        return currentStream;
    }

    private void addSinks(DataStream<DataRecord> stream) {
        List<PipelineConfiguration.Sink> sinks = config.getSinks();

        if (sinks == null || sinks.isEmpty()) {
            System.out.println("No sinks configured, adding console sink");
            stream.print();
            return;
        }

        for (PipelineConfiguration.Sink sinkConfig : sinks) {
            String sinkType = sinkConfig.getType();
            Boolean enabled = sinkConfig.getEnabled();

            if (enabled == null || !enabled) {
                System.out.println("Skipping disabled sink: " + sinkType);
                continue;
            }

            System.out.println("Adding sink: " + sinkType);
            SinkPlugin sink = sinkPlugins.get(sinkType);
            if (sink == null) {
                System.err.println("Unknown sink type: " + sinkType);
                continue;
            }

            // Configure sink
            Map<String, Object> pluginConfig = sinkConfig.getConfig();
            sink.configure(pluginConfig);

            // Add sink
            sink.addSink(stream);
        }
    }

    private void configure(StreamExecutionEnvironment env) {
        PipelineConfiguration.Pipeline pipeline = config.getPipeline();

        if (pipeline != null) {
            // Set parallelism
            Integer parallelism = pipeline.getParallelism();
            if (parallelism != null) {
                env.setParallelism(parallelism);
            }

            // Configure checkpointing
            PipelineConfiguration.PipelineCheckPointConfig checkpointing = pipeline.getCheckpointing();
            if (checkpointing != null) {
                Boolean enabled = checkpointing.getEnabled();
                Integer interval = checkpointing.getInterval();

                if (enabled != null && enabled && interval != null) {
                    env.enableCheckpointing(interval);
                    System.out.println("Checkpointing enabled with interval: " + interval + "ms");
                }
            }
        }
    }

    private void register() {
        System.out.println("Registering plugins...");
        sourcePlugins.put("kafka", new KafkaSourcePlugin());

        processorPlugins.put("user_address_enrichment", new UserAddressEnrichmentProcessor());
        processorPlugins.put("deduplication-processor", new LatestUserDeduplicationProcessor());

        sinkPlugins.put("kafka", new KafkaSinkPlugin());

        System.out.println("Registered " + sourcePlugins.size() + " source plugins");
        System.out.println("Registered " + processorPlugins.size() + " processor plugins");
        System.out.println("Registered " + sinkPlugins.size() + " sink plugins");
    }

    private PipelineConfiguration loadConfig() {
        try {
            String configPath = params.get("conf", "/opt/flink/usrlib/config/pipeline.yaml");
            System.out.println("Loading configuration from: " + configPath);
            return PipelineConfigReader.fromFile(configPath);
        } catch (Exception e) {
            System.err.println("Failed to load config: " + e.getMessage());
            throw new RuntimeException("Error loading configuration", e);
        }
    }
}
