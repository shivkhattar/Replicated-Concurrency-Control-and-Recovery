package com.nyu.repcrec.service;

import com.nyu.repcrec.util.FileUtils;
import lombok.AccessLevel;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Setter;

import java.util.HashSet;
import java.util.Set;

@Data
@EqualsAndHashCode
public class Site {

    @Setter(AccessLevel.PRIVATE)
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
        Set<String> abortTransactions = new HashSet<>();
        lockManager.getReadLocks().forEach((variable, transactions) ->
                transactions.forEach(transaction -> {
                    dataManager.moveValueBackToCommittedValueAtTime(variable, transaction.getTimestamp(), siteId);
                    transaction.setTransactionStatus(TransactionStatus.ABORT);
                    abortTransactions.add("T" + transaction.getTransactionId());
                })
        );
        lockManager.getWriteLocks().forEach((variable, transaction) -> {
                    dataManager.moveValueBackToCommittedValueAtTime(variable, transaction.getTimestamp(), siteId);
                    transaction.setTransactionStatus(TransactionStatus.ABORT);
                    abortTransactions.add("T" + transaction.getTransactionId());
                }
        );
        lockManager.eraseAllLocks();
        dataManager.fail();
        FileUtils.log("Site" + siteId + " failed! All locks released!");
        if (!abortTransactions.isEmpty()) FileUtils.log(abortTransactions.toString() + " will abort when they end");
    }

    public void recover() {
        isUp = true;
        dataManager.recover();
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

    public void commitValues(Transaction transaction, Integer currentTimestamp) {
        Set<Integer> writeVariables = lockManager.getWriteVariablesHeldByTransaction(transaction);
        writeVariables.forEach(writeVariable -> dataManager.commitValueForVariable(writeVariable, currentTimestamp, siteId));
    }

    public void moveCurrentValuesBackToCommitted(Transaction transaction, Integer currentTimestamp) {
        Set<Integer> writeVariables = lockManager.getWriteVariablesHeldByTransaction(transaction);
        writeVariables.forEach(writeVariable -> dataManager.moveValueBackToCommittedValueAtTime(writeVariable, currentTimestamp, siteId));
    }

    public boolean isReadAllowed(Integer variable) {
        return isUp && dataManager.isReadAllowed(variable);
    }
}