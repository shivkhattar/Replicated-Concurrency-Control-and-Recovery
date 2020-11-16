package com.nyu.repcrec.service;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode
public class Site {

    private Integer siteId;
    private boolean isUp;
    private LockManager lockManager;
    private DataManager dataManager;

    public Site(int id) {
        this.siteId = id;
        isUp = true;
        lockManager = new LockManager();
        dataManager = new DataManager();
    }

    public void addDataValue(Integer key, Integer value) {
        dataManager.insertData(key, value);
    }

    public void fail() {

    }

    public void recover() {

    }
}
