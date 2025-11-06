package org.example.flink.event;

import java.util.HashMap;
import java.util.Map;

// Entity classes
public class UserEntity {
    private String userId;
    private String email;
    private String name;
    private Map<String, Object> fields;

    public static UserEntity fromDataRecord(DataRecord record) {
        UserEntity user = new UserEntity();
        user.userId = record.getFieldAsString("user_id");
        user.email = record.getFieldAsString("email");
        user.name = record.getFieldAsString("name");
        user.fields = new HashMap<>(record.getFields());
        return user;
    }

    // Getters
    public String getUserId() {
        return userId;
    }

    public String getEmail() {
        return email;
    }

    public String getName() {
        return name;
    }

    public Map<String, Object> getFields() {
        return fields;
    }
}
