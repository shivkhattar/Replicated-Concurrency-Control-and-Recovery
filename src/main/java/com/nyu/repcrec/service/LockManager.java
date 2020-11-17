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

    public void addReadLock(Transaction transaction, Integer variable) {
        List<Transaction> transactions = readLocks.getOrDefault(variable, new ArrayList<>());
        transactions.add(transaction);
        readLocks.put(variable, transactions);
    }

    public Optional<Transaction> waitsForWriteTransaction(Integer variable) {
        return Optional.ofNullable(writeLocks.get(variable));
    }

    public void eraseLocks() {
        readLocks.clear();
        writeLocks.clear();
    }
}
