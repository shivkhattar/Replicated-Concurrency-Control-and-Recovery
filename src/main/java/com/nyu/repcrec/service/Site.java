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

    public void addDataValue(Integer variable, Integer value) {
        dataManager.insertData(variable, value);
    }

    public void fail() {

    }

    public void recover() {

    }

    public Integer readValue(Transaction transaction, Integer variable) {
        if (!transaction.isReadOnly()) lockManager.addReadLock(transaction, variable);
        return dataManager.readValue(variable, transaction.isReadOnly());
    }

}
