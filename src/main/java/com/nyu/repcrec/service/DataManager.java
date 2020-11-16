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

    public void insertData(Integer key, Integer value) {
        data.put(key, new DataValue(value, value));
    }
}
