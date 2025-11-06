package org.example.flink.beans;

import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
public class PipelineConfiguration {
    private Pipeline pipeline;
    private Source source;
    private List<Processor> processors;
    private List<Sink> sinks;

    @Data
    public static class Pipeline {
        private String name;
        private Integer parallelism;
        private PipelineCheckPointConfig checkpointing;
    }

    @Data
    public static class PipelineCheckPointConfig {
        private Boolean enabled;
        private Integer interval;
    }

    @Data
    public static class Source {
        private String type;
        private Map<String, Object> config;
    }

    @Data
    public static class Processor {
        private String type;
        private Map<String, Object> config;
        private Boolean enabled;
    }

    @Data
    public static class Sink {
        private String type;
        private Map<String, Object> config;
        private Boolean enabled;
    }
}
