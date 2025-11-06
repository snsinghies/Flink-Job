package org.example.flink.plugins;

import java.util.HashMap;
import java.util.Map;

public class PluginConfig {
    private final Map<String, Object> config;

    public PluginConfig(Map<String, Object> config) {
        this.config = config != null ? config : new HashMap<>();
    }

    public String getString(String key) {
        return getString(key, null);
    }

    public String getString(String key, String defaultValue) {
        Object value = config.get(key);
        return value != null ? value.toString() : defaultValue;
    }

    public Integer getInt(String key) {
        return getInt(key, null);
    }

    public Integer getInt(String key, Integer defaultValue) {
        Object value = config.get(key);
        if (value instanceof Integer) return (Integer) value;
        if (value instanceof String) {
            try {
                return Integer.parseInt((String) value);
            } catch (NumberFormatException e) {
                return defaultValue;
            }
        }
        return defaultValue;
    }

    public Boolean getBoolean(String key) {
        return getBoolean(key, null);
    }

    public Boolean getBoolean(String key, Boolean defaultValue) {
        Object value = config.get(key);
        if (value instanceof Boolean) return (Boolean) value;
        if (value instanceof String) return Boolean.parseBoolean((String) value);
        return defaultValue;
    }

    @SuppressWarnings("unchecked")
    public java.util.List<String> getStringList(String key) {
        Object value = config.get(key);
        if (value instanceof java.util.List) {
            return (java.util.List<String>) value;
        }
        return new java.util.ArrayList<>();
    }

    @SuppressWarnings("unchecked")
    public java.util.List<Map<String, Object>> getMapList(String key) {
        Object value = config.get(key);
        if (value instanceof java.util.List) {
            return (java.util.List<Map<String, Object>>) value;
        }
        return new java.util.ArrayList<>();
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> getMap(String key) {
        Object value = config.get(key);
        if (value instanceof Map) {
            return (Map<String, Object>) value;
        }
        return new java.util.HashMap<>();
    }

    public boolean hasKey(String key) {
        return config.containsKey(key);
    }

    public Map<String, Object> getAll() {
        return new java.util.HashMap<>(config);
    }
}
