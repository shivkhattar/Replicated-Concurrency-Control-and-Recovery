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

    public void insertData(Integer variable, Integer timestamp, Integer value) {
        data.put(variable, new DataValue(value, timestamp, value));
    }

    public Integer readValue(Integer variable, boolean readCommittedValue, Integer timestamp) {
        if (!data.containsKey(variable)) {
            throw new RepCRecException("Invalid variable accessed in readValue!");
        }
        DataValue values = data.get(variable);
        return readCommittedValue ? values.getLastCommittedValues().floorEntry(timestamp).getValue() : values.getCurrentValue();
    }

    public void moveValueBackToCommittedValueAtTime(Integer variable, Integer timestamp) {
        if (!data.containsKey(variable)) {
            throw new RepCRecException("Invalid variable accessed in moveValueBackToCommittedValueAtTime!");
        }
        DataValue values = data.get(variable);
        values.setCurrentValue(values.getLastCommittedValues().floorEntry(timestamp).getValue());
    }
}
