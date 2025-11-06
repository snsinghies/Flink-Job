package org.example.flink.event;

import lombok.Data;

import java.io.Serializable;
import java.util.Map;

@Data
public class DataRecord implements Serializable {
    private Map<String, Object> fields;
    private String sourceType;
    private long timestamp;
    private Map<String, Object> metadata;

    public DataRecord() {
        this.fields = new java.util.HashMap<>();
        this.metadata = new java.util.HashMap<>();
        this.timestamp = System.currentTimeMillis();
    }

    public DataRecord(Map<String, Object> fields) {
        this();
        this.fields = fields != null ? fields : new java.util.HashMap<>();
    }

    // Utility methods
    public Object getField(String key) {
        return fields.get(key);
    }

    public String getFieldAsString(String key) {
        Object value = getField(key);
        return value != null ? value.toString() : null;
    }

    public Integer getFieldAsInt(String key) {
        Object value = getField(key);
        if (value instanceof Integer) return (Integer) value;
        if (value instanceof String) {
            try {
                return Integer.parseInt((String) value);
            } catch (NumberFormatException e) {
                return null;
            }
        }
        return null;
    }

    public Double getFieldAsDouble(String key) {
        Object value = getField(key);
        if (value instanceof Double) return (Double) value;
        if (value instanceof Number) return ((Number) value).doubleValue();
        if (value instanceof String) {
            try {
                return Double.parseDouble((String) value);
            } catch (NumberFormatException e) {
                return null;
            }
        }
        return null;
    }

    @Override
    public String toString() {
        return "DataRecord{" +
                "fields=" + fields +
                ", sourceType='" + sourceType + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
