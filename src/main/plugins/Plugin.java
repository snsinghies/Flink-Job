package org.example.flink.plugins;

import java.io.Serializable;
import java.util.Map;

public interface Plugin extends Serializable {
    String getType();
    String getDescription();
    void configure(Map<String, Object> config);
    boolean isConfigured();
}
