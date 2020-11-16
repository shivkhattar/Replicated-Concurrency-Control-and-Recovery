package com.nyu.repcrec.service;

import lombok.EqualsAndHashCode;

import java.util.List;
import java.util.Map;

@EqualsAndHashCode
public class TransactionManager {

    private Integer currentTimestamp;
    private List<Transaction> transactions;
    private List<Site> sites;
    private Map<Integer, List<Transaction>> waitingTransactions;
    private Map<Integer, List<Site>> waitingSites;
    private Map<Integer, List<Operation>> waitingOperations;
    private List<Integer> visitedTransactions;


}
