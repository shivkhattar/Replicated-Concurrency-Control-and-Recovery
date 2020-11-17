package com.nyu.repcrec.service;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.HashMap;
import java.util.Map;

@Data
@EqualsAndHashCode
public class DataManager {

    private Map<Integer, DataValue> data;

    public DataManager() {
        data = new HashMap<>();
    }

    public void insertData(Integer variable, Integer value) {
        data.put(variable, new DataValue(value, value));
    }

    public Integer readValue(Integer variable, boolean readCommittedValue) {
        if (!data.containsKey(variable)) {
            throw new RepCRecException("Invalid variable accessed!");
        }
        DataValue values = data.get(variable);
        return readCommittedValue ? values.getLastCommittedValue() : values.getCurrentValue();
    }
}
