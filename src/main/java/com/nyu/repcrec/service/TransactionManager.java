package com.nyu.repcrec.service;

import com.nyu.repcrec.util.FileUtils;
import lombok.EqualsAndHashCode;

import java.util.*;

@EqualsAndHashCode
public class TransactionManager {

    private Integer currentTimestamp;
    private List<Transaction> transactions;
    private List<Site> sites;
    private Map<Integer, List<Transaction>> waitingTransactions;
    private Map<Integer, List<Site>> waitingSites;
    private Map<Integer, List<Operation>> waitingOperations;
    private List<Integer> visitedTransactions;

    public TransactionManager() {
        currentTimestamp = 1;
        transactions = new ArrayList<>();
        sites = new ArrayList<>();
        waitingTransactions = new HashMap<>();
        waitingSites = new HashMap<>();
        waitingOperations = new HashMap<>();
        visitedTransactions = new ArrayList<>();

        //Create 11 new sites, first one being the dummy site
        for (int i = 0; i <= 10; i++) sites.add(new Site(i));

        //create new variables and place them at the correct site
        for (int variable = 1; variable <= 20; variable++) {
            if (variable % 2 == 0) addVariableAtAllSites(variable);
            else addVariableAtSiteId(variable, (variable + 1) % 10);
        }
    }

    private void addVariableAtAllSites(int variable) {
        sites.forEach(site -> site.addDataValue(variable, 10 * variable));
    }

    private void addVariableAtSiteId(int variable, int siteid) {
        sites.get(siteid).addDataValue(variable, 10 * variable);
    }

    public void executeOperation(Operation operation) {
        switch (operation.getOperationType()) {
            case BEGIN:
                beginTransaction(operation, false, currentTimestamp);
                break;
            case BEGINRO:
                beginTransaction(operation, true, currentTimestamp);
                break;
            case END:
                endTransaction(operation.getTransactionId());
                break;
            case READ:
                break;
            case WRITE:
                break;
            case DUMP:
                break;
            case FAIL:
                sites.get(operation.getSiteId()).fail();
                break;
            case RECOVER:
                sites.get(operation.getSiteId()).recover();
                break;
        }
        currentTimestamp++;
    }

    private void beginTransaction(Operation operation, boolean isReadOnly, Integer currentTimestamp) {
        if (getTransaction(operation.getTransactionId()).isPresent()) {
            throw new RepCRecException("Transaction with id: " + operation.getTransactionId() + " already exists!");
        }
        FileUtils.log("Begin" + (isReadOnly ? " Read Only " : " ") + "Transaction: " + operation.getTransactionId() + " at time: " + currentTimestamp);
        transactions.add(new Transaction(operation.getTransactionId(), currentTimestamp, operation, isReadOnly, TransactionStatus.ACTIVE));
    }

    private void endTransaction(Integer transactionId) {
        Transaction transaction = getTransaction(transactionId).orElseThrow(() -> new RepCRecException("Transaction with id:" + transactionId + " not found"));
        if (TransactionStatus.ABORTED.equals(transaction.getTransactionStatus())) abort(transaction);
        else if (TransactionStatus.ACTIVE.equals(transaction.getTransactionStatus())) commit(transaction);
    }

    private Optional<Transaction> getTransaction(Integer transactionId) {
        return transactions.stream().filter(transaction -> transaction.getTransactionId().equals(transactionId)).findFirst();
    }

    private void abort(Transaction transaction) {

    }

    private void commit(Transaction transaction) {

    }

}
