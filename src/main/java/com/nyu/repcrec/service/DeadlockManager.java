package com.nyu.repcrec.service;

import lombok.RequiredArgsConstructor;

import java.util.*;

@RequiredArgsConstructor
public class DeadlockManager {

    private final Map<Integer, List<Transaction>> waitsForGraph;

    private Transaction youngestTransaction = null;

    public Optional<Transaction> findYoungestDeadlockedTransaction(Transaction transaction){
        if(transaction == null){
            throw new RepCRecException("Transaction cannot be null for a detecting deadlock");
        }

        Set<Integer> visited = new HashSet<>();

        youngestTransaction = transaction;

        return detectCycle(visited, transaction) ? Optional.of(youngestTransaction) : Optional.empty();
    }

    private boolean detectCycle(Set<Integer> visited, Transaction currentTransaction){

        if(currentTransaction.getTimestamp() > youngestTransaction.getTimestamp()){
            youngestTransaction = currentTransaction;
        }

        visited.add(currentTransaction.getTransactionId());

        List<Transaction> connectedTransactions = waitsForGraph.getOrDefault(currentTransaction.getTransactionId(), new LinkedList<>());

        for(Transaction nextTransaction : connectedTransactions){
            if(!visited.contains(nextTransaction.getTransactionId())){
                if(detectCycle(visited, nextTransaction)) {
                    return true;
                }
            } else {
                return true;
            }
        }

        return false;
    }

}
