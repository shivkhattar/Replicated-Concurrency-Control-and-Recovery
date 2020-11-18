package com.nyu.repcrec.service;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.*;

@Data
@EqualsAndHashCode
public class LockManager {

    private Map<Integer, List<Transaction>> readLocks;
    private Map<Integer, Transaction> writeLocks;

    public LockManager() {
        readLocks = new HashMap<>();
        writeLocks = new HashMap<>();
    }

    public boolean canRead(Transaction transaction, Integer variable) {
        return !writeLocks.containsKey(variable) || writeLocks.get(variable).equals(transaction);
    }

    public boolean canWrite(Transaction transaction, Integer variable) {
        return (!writeLocks.containsKey(variable) || writeLocks.get(variable).equals(transaction))
                && (!readLocks.containsKey(variable) ||
                (readLocks.containsKey(variable) && readLocks.get(variable).size() == 1 && readLocks.get(variable).contains(transaction)));
    }

    public void addReadLock(Transaction transaction, Integer variable) {
        List<Transaction> transactions = readLocks.getOrDefault(variable, new ArrayList<>());
        transactions.add(transaction);
        readLocks.put(variable, transactions);
    }

    public void addWriteLock(Transaction transaction, Integer variable) {
        writeLocks.put(variable, transaction);
    }

    public Optional<Transaction> waitsForWriteTransaction(Integer variable) {
        return Optional.ofNullable(writeLocks.get(variable));
    }

    public List<Transaction> waitsForReadTransaction(Integer variable) {
        return readLocks.getOrDefault(variable, new ArrayList<>());
    }

    public void eraseLocks() {
        readLocks.clear();
        writeLocks.clear();
    }
}
