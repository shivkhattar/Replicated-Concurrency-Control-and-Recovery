package com.nyu.repcrec.service;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.TreeMap;

@Data
@EqualsAndHashCode
public class DataValue {
    private Integer currentValue;
    private TreeMap<Integer, Integer> lastCommittedValues;

    public DataValue(Integer currentValue, Integer timestamp, Integer committedValue) {
        this.currentValue = currentValue;
        this.lastCommittedValues = new TreeMap<>();
        lastCommittedValues.put(timestamp, committedValue);
    }

    protected void insertNewCommittedValue(Integer timestamp, Integer committedValue) {
        if (lastCommittedValues == null) lastCommittedValues = new TreeMap<>();
        lastCommittedValues.put(timestamp, committedValue);
    }
}
