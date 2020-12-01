package com.nyu.repcrec.service;

import lombok.EqualsAndHashCode;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@EqualsAndHashCode
public class Transactions {

    private Map<Integer, Transaction> transactions;

    public Transactions() {
        transactions = new HashMap<>();
    }

    protected void put(Transaction transaction) {
        transactions.put(transaction.getTransactionId(), transaction);
    }

    protected Optional<Transaction> get(Integer transactionId) {
        return Optional.ofNullable(transactions.get(transactionId));
    }
}
