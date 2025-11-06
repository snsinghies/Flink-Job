package org.example.flink.event;

import java.util.HashMap;
import java.util.Map;

public class AddressEntity {
        private String addressId;
        private String userId;
        private String street;
        private String city;
        private Map<String, Object> fields;

        public static AddressEntity fromDataRecord(DataRecord record) {
            AddressEntity address = new AddressEntity();
            address.addressId = record.getFieldAsString("address_id");
            address.userId = record.getFieldAsString("user_id");
            address.street = record.getFieldAsString("street");
            address.city = record.getFieldAsString("city");
            address.fields = new HashMap<>(record.getFields());
            return address;
        }

        // Getters
        public String getAddressId() { return addressId; }
        public String getUserId() { return userId; }
        public String getStreet() { return street; }
        public String getCity() { return city; }
        public Map<String, Object> getFields() { return fields; }
    }
