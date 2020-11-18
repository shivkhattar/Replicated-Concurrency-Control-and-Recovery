package com.nyu.repcrec.service;

import com.nyu.repcrec.util.FileUtils;
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

    public void addDataValue(Integer variable, Integer timestamp, Integer value) {
        dataManager.insertData(variable, timestamp, value);
    }

    public void fail() {
        isUp = false;
        lockManager.getReadLocks().forEach((variable, transactions) ->
                transactions.forEach(transaction -> {
                    dataManager.moveValueBackToCommittedValueAtTime(variable, transaction.getTimestamp());
                    transaction.setTransactionStatus(TransactionStatus.ABORT);
                })
        );
        lockManager.getWriteLocks().forEach((variable, transaction) -> {
                    dataManager.moveValueBackToCommittedValueAtTime(variable, transaction.getTimestamp());
                    transaction.setTransactionStatus(TransactionStatus.ABORT);
                }
        );
        lockManager.eraseLocks();
        FileUtils.log("Site" + siteId + " failed! All locks released!");
    }

    public void recover() {
        isUp = true;
        FileUtils.log("Site" + siteId + " recovered!");
    }

    public Integer readValue(Transaction transaction, Integer variable) {
        if (!transaction.isReadOnly()) lockManager.addReadLock(transaction, variable);
        return dataManager.readValue(variable, transaction.isReadOnly(), transaction.getTimestamp());
    }

    public void writeValue(Transaction transaction, Integer variable, Integer writeValue) {
        lockManager.addWriteLock(transaction, variable);
        dataManager.writeValue(variable, writeValue);
    }

}
