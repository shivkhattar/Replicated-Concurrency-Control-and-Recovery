package com.nyu.repcrec.service;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.*;
import java.util.stream.Collectors;

@Data
@EqualsAndHashCode
public class LockManager {

    private Map<Integer, List<Transaction>> readLocks;
    private Map<Integer, Transaction> writeLocks;

    public LockManager() {
        readLocks = new HashMap<>();
        writeLocks = new HashMap<>();
    }

    protected boolean canRead(Transaction transaction, Integer variable) {
        return !writeLocks.containsKey(variable) || writeLocks.get(variable).equals(transaction);
    }

    protected boolean canWrite(Transaction transaction, Integer variable) {
        return (!writeLocks.containsKey(variable) || writeLocks.get(variable).equals(transaction))
                && (!readLocks.containsKey(variable) ||
                (readLocks.containsKey(variable) && readLocks.get(variable).size() == 1 && readLocks.get(variable).contains(transaction)));
    }

    protected void addReadLock(Transaction transaction, Integer variable) {
        List<Transaction> transactions = readLocks.getOrDefault(variable, new ArrayList<>());
        transactions.add(transaction);
        readLocks.put(variable, transactions);
    }

    protected void addWriteLock(Transaction transaction, Integer variable) {
        writeLocks.put(variable, transaction);
    }

    protected Optional<Transaction> waitsForWriteTransaction(Integer variable) {
        return Optional.ofNullable(writeLocks.get(variable));
    }

    protected List<Transaction> waitsForReadTransaction(Integer variable) {
        return readLocks.getOrDefault(variable, new ArrayList<>());
    }

    protected void eraseAllLocks() {
        readLocks.clear();
        writeLocks.clear();
    }

    protected void eraseLocksForTransaction(Transaction transaction) {
        readLocks.forEach((variable, transactions) -> transactions.remove(transaction));
        readLocks.entrySet().removeIf(readLock -> readLock.getValue().isEmpty());
        writeLocks.entrySet().removeIf(writeLock -> writeLock.getValue().equals(transaction));
    }

    protected Set<Integer> getAllVariablesHeldByTransaction(Transaction transaction) {
        Set<Integer> variables = new LinkedHashSet<>();
        variables.addAll(readLocks.entrySet().stream().filter(entry -> entry.getValue().contains(transaction)).map(Map.Entry::getKey).collect(Collectors.toSet()));
        variables.addAll(getWriteVariablesHeldByTransaction(transaction));
        return variables;
    }

    protected Set<Integer> getWriteVariablesHeldByTransaction(Transaction transaction) {
        return writeLocks.entrySet().stream().filter(entry -> entry.getValue().equals(transaction)).map(Map.Entry::getKey).collect(Collectors.toSet());
    }
}