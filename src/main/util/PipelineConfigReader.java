package org.example.flink.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.example.flink.beans.PipelineConfiguration;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

public class PipelineConfigReader {
    private static final ObjectMapper YAML_MAPPER = new ObjectMapper(new YAMLFactory());

    /**
     * Load configuration from YAML file
     */
    public static PipelineConfiguration fromFile(String filePath) throws IOException {
        return YAML_MAPPER.readValue(new File(filePath), PipelineConfiguration.class);
    }

    /**
     * Load configuration from YAML string
     */
    public static PipelineConfiguration fromString(String yamlContent) throws IOException {
        return YAML_MAPPER.readValue(yamlContent, PipelineConfiguration.class);
    }

    /**
     * Load configuration from InputStream (useful for resources)
     */
    public static PipelineConfiguration fromInputStream(InputStream inputStream) throws IOException {
        return YAML_MAPPER.readValue(inputStream, PipelineConfiguration.class);
    }

    /**
     * Load configuration from classpath resource
     */
    public static PipelineConfiguration fromResource(String resourcePath) throws IOException {
        InputStream inputStream = PipelineConfigReader.class.getClassLoader()
                .getResourceAsStream(resourcePath);
        if (inputStream == null) {
            throw new IOException("Resource not found: " + resourcePath);
        }
        return fromInputStream(inputStream);
    }

    /**
     * Convert configuration back to YAML string
     */
    public static String toYamlString(PipelineConfiguration config) throws IOException {
        return YAML_MAPPER.writeValueAsString(config);
    }

    /**
     * Save configuration to YAML file
     */
    public static void toFile(PipelineConfiguration config, String filePath) throws IOException {
        YAML_MAPPER.writeValue(new File(filePath), config);
    }
}
