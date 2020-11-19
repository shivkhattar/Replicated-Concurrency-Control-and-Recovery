package com.nyu.repcrec.service;

import lombok.RequiredArgsConstructor;

import java.util.*;

@RequiredArgsConstructor
public class DeadlockManager {

    private final Map<Integer, List<Transaction>> waitsForGraph;

    private Transaction youngestTransaction = null;

    public Optional<Transaction> detectDeadlock(Transaction transaction){

        Set<Integer> visited = new HashSet<>();

        youngestTransaction = transaction;

        return detectCycle(visited, transaction) ? Optional.of(youngestTransaction) : Optional.empty();
    }

    private boolean detectCycle(Set<Integer> visited, Transaction currentTransaction){

        if(currentTransaction.getTimestamp() > youngestTransaction.getTimestamp()){
            youngestTransaction = currentTransaction;
        }

        visited.add(currentTransaction.getTransactionId());

        List<Transaction> list = waitsForGraph.getOrDefault(currentTransaction.getTransactionId(), new LinkedList<>());

        for(Transaction t : list){
            if(!visited.contains(t.getTransactionId())){
                if(detectCycle(visited, t)) {
                    return true;
                }
            } else {
                return true;
            }
        }

        return false;
    }

}
