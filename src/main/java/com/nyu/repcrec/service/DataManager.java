package com.nyu.repcrec.service;

import com.nyu.repcrec.util.FileUtils;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

@Data
@EqualsAndHashCode
public class DataManager {

    private TreeMap<Integer, DataValue> data;
    private Map<Integer, Boolean> availableForRead;

    public DataManager() {
        data = new TreeMap<>();
        availableForRead = new HashMap<>();
    }

    protected void insertData(Integer variable, Integer timestamp, Integer value) {
        data.put(variable, new DataValue(value, timestamp, value));
        availableForRead.put(variable, true);
    }

    protected Integer readValue(Integer variable, boolean readCommittedValue, Integer timestamp) {
        if (!data.containsKey(variable)) {
            throw new RepCRecException("Invalid variable accessed in readValue!");
        }
        DataValue dataValue = data.get(variable);
        return readCommittedValue ? dataValue.getLastCommittedValues().floorEntry(timestamp).getValue() : dataValue.getCurrentValue();
    }

    protected void writeValue(Integer variable, Integer writeValue) {
        if (!data.containsKey(variable)) {
            throw new RepCRecException("Invalid variable accessed in readValue!");
        }
        DataValue values = data.get(variable);
        values.setCurrentValue(writeValue);
    }

    protected void commitValueForVariable(Integer variable, Integer currentTimestamp, Integer siteId) {
        if (!data.containsKey(variable)) {
            throw new RepCRecException("Invalid variable accessed in moveValueBackToCommittedValueAtTime!");
        }
        DataValue values = data.get(variable);
        values.insertNewCommittedValue(currentTimestamp, values.getCurrentValue());
        availableForRead.put(variable, true);
        FileUtils.log(currentTimestamp + ": x" + variable + "=" + values.getCurrentValue() + " at site" + siteId);

    }

    protected void moveValueBackToCommittedValueAtTime(Integer variable, Integer timestamp, Integer siteId) {
        if (!data.containsKey(variable)) {
            throw new RepCRecException("Invalid variable accessed in moveValueBackToCommittedValueAtTime!");
        }
        DataValue values = data.get(variable);
        Integer committedValue = values.getLastCommittedValues().floorEntry(timestamp).getValue();
        FileUtils.log("Rollback: x" + variable + "=" + values.getCurrentValue() + " to " + committedValue + " at site" + siteId);
        values.setCurrentValue(committedValue);
    }

    protected void fail() {
        availableForRead.keySet().forEach(variable -> availableForRead.put(variable, false));
    }

    protected void recover() {
        availableForRead.keySet().forEach(variable -> {
            if (variable % 2 == 1) availableForRead.put(variable, true);
        });
    }

    protected boolean isReadAllowed(Integer variable) {
        return availableForRead.getOrDefault(variable, false);
    }
}
