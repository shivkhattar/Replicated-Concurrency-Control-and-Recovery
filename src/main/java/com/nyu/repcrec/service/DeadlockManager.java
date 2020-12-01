package com.nyu.repcrec.service;

import java.util.*;

public class DeadlockManager {

    private static DeadlockManager deadlockManager = null;

    private Transaction youngestTransaction;

    private DeadlockManager() {
    }

    public static DeadlockManager getInstance() {
        if (deadlockManager == null) {
            deadlockManager = new DeadlockManager();
        }
        return deadlockManager;
    }

    protected Optional<Transaction> findYoungestDeadlockedTransaction(Transaction transaction,
                                                                      final Map<Integer, List<Transaction>> waitsForGraph) {
        if (transaction == null) {
            throw new RepCRecException("Transaction cannot be null for a detecting deadlock");
        }

        Set<Integer> visited = new HashSet<>();

        youngestTransaction = transaction;

        return detectCycle(visited, transaction, waitsForGraph) ? Optional.of(youngestTransaction) : Optional.empty();
    }

    private boolean detectCycle(Set<Integer> visited, Transaction currentTransaction, final Map<Integer, List<Transaction>> waitsForGraph) {

        if (currentTransaction.getTimestamp() > youngestTransaction.getTimestamp()) {
            youngestTransaction = currentTransaction;
        }

        visited.add(currentTransaction.getTransactionId());

        List<Transaction> connectedTransactions = waitsForGraph.getOrDefault(currentTransaction.getTransactionId(), new LinkedList<>());

        for (Transaction nextTransaction : connectedTransactions) {
            if (!visited.contains(nextTransaction.getTransactionId())) {
                if (detectCycle(visited, nextTransaction, waitsForGraph)) {
                    return true;
                }
            } else {
                return true;
            }
        }

        return false;
    }
}
