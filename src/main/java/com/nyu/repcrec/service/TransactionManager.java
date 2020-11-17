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

    private static final Integer MIN_SITE_ID = 1;
    private static final Integer MAX_SITE_ID = 10;
    private static final Integer MIN_VARIABLE = 1;
    private static final Integer MAX_VARIABLE = 20;

    public TransactionManager() {
        currentTimestamp = 0;
        transactions = new ArrayList<>();
        sites = new ArrayList<>();
        waitingTransactions = new HashMap<>();
        waitingSites = new HashMap<>();
        waitingOperations = new HashMap<>();
        visitedTransactions = new ArrayList<>();

        //Create 11 new sites, first one being the dummy site
        for (int i = 0; i <= MAX_SITE_ID; i++) sites.add(new Site(i));

        //create new variables and place them at the correct site
        for (int variable = MIN_VARIABLE; variable <= MAX_VARIABLE; variable++) {
            if (isOddVariable(variable)) addVariableAtSiteId(variable, getSiteIdForOddVariable(variable));
            else addVariableAtAllSites(variable);
        }
    }

    private void addVariableAtAllSites(int variable) {
        sites.forEach(site -> site.addDataValue(variable, currentTimestamp, 10 * variable));
    }

    private void addVariableAtSiteId(int variable, int siteid) {
        sites.get(siteid).addDataValue(variable, currentTimestamp, 10 * variable);
    }

    private boolean isOddVariable(Integer variable) {
        return variable % 2 == 1;
    }

    private Integer getSiteIdForOddVariable(Integer variable) {
        return (variable + 1) % 10;
    }

    public void executeOperation(Operation operation) {
        currentTimestamp++;
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
                read(operation);
                break;
            case WRITE:
                break;
            case DUMP:
                break;
            case FAIL:
                sites.get(operation.getSiteId()).fail();
                break;
            case RECOVER:
                Site site = sites.get(operation.getSiteId());
                site.recover();
                wakeupTransactionsWaitingForSite(site);
                break;
        }
    }

    private void beginTransaction(Operation operation, boolean isReadOnly, Integer currentTimestamp) {
        if (getTransaction(operation.getTransactionId()).isPresent()) {
            throw new RepCRecException("Transaction with id: " + operation.getTransactionId() + " already exists!");
        }
        FileUtils.log("Begin" + (isReadOnly ? " Read Only " : " ") + "Transaction: " + operation.getTransactionId() + " at time: " + currentTimestamp);
        transactions.add(new Transaction(operation.getTransactionId(), currentTimestamp, operation, isReadOnly, TransactionStatus.ACTIVE));
    }

    private void endTransaction(Integer transactionId) {
        Transaction transaction = getTransactionOrThrowException(transactionId);
        if (TransactionStatus.ABORTED.equals(transaction.getTransactionStatus())) abort(transaction);
        else if (TransactionStatus.ACTIVE.equals(transaction.getTransactionStatus())) commit(transaction);
    }

    private Transaction getTransactionOrThrowException(Integer transactionId) {
        return getTransaction(transactionId).orElseThrow(() -> new RepCRecException("Transaction with id:" + transactionId + " not found"));
    }

    private Optional<Transaction> getTransaction(Integer transactionId) {
        return transactions.stream().filter(transaction -> transaction.getTransactionId().equals(transactionId)).findFirst();
    }

    private void abort(Transaction transaction) {

    }

    private void commit(Transaction transaction) {

    }

    private void read(Operation operation) {
        Transaction transaction = getTransactionOrThrowException(operation.getTransactionId());
        Integer variable = operation.getVariable();
        transaction.setCurrentOperation(operation);
        if (transaction.isReadOnly()) {
            readForReadOnlyTransaction(transaction, variable);
        } else {
            read(transaction, variable, operation);
        }
    }

    private void write(Operation operation) {
    }

    private void readForReadOnlyTransaction(Transaction transaction, Integer variable) {
        if (isOddVariable(variable)) {
            readForReadOnlyTransactionForOddVariable(transaction, variable);
        } else {
            readForReadOnlyTransactionForEvenVariable(transaction, variable);
        }
    }

    private void read(Transaction transaction, Integer variable, Operation operation) {
        if (isOddVariable(variable)) {
            readForOddVariable(transaction, variable, operation);
        } else {
            readForEvenVariable(transaction, variable, operation);
        }
    }

    private void readForReadOnlyTransactionForOddVariable(Transaction transaction, Integer variable) {
        Site site = sites.get(getSiteIdForOddVariable(variable));
        if (site.isUp()) {
            readVariable(transaction, variable, site);
        } else {
            addWaitingSite(transaction, site);
        }
    }

    private void readForReadOnlyTransactionForEvenVariable(Transaction transaction, Integer variable) {
        for (int i = MIN_SITE_ID; i <= MAX_SITE_ID; i++) {
            Site site = sites.get(i);
            if (site.isUp()) {
                readVariable(transaction, variable, site);
                return;
            }
        }
        sites.stream().skip(MIN_SITE_ID).forEach(site -> addWaitingSite(transaction, site));
    }

    private void readForOddVariable(Transaction transaction, Integer variable, Operation operation) {
        Site site = sites.get(getSiteIdForOddVariable(variable));
        if (site.isUp() && site.getLockManager().canRead(transaction, variable)) {
            //TODO: Add write starvation check
            readVariable(transaction, variable, site);
        } else {
            block(transaction, site, variable, operation);
        }
    }

    private void readForEvenVariable(Transaction transaction, Integer variable, Operation operation) {
        for (int i = MIN_SITE_ID; i <= MAX_SITE_ID; i++) {
            Site site = sites.get(i);
            if (site.isUp() && site.getLockManager().canRead(transaction, variable)) {
                //TODO: Add write starvation check
                readVariable(transaction, variable, site);
                return;
            }
        }
        sites.stream().skip(MIN_SITE_ID).forEach(site -> block(transaction, site, variable, operation));
    }


    private void readVariable(Transaction transaction, Integer variable, Site site) {
        Integer value = site.readValue(transaction, variable);
        FileUtils.log("Transaction: + " + transaction.getTransactionId() + "Read value for x" + variable + " from site:" + site.getSiteId() + " =" + value);
    }

    private void block(Transaction transaction, Site site, Integer variable, Operation operation) {
        if (!site.isUp()) {
            addWaitingSite(transaction, site);
        } else {
            if (operation.getOperationType().equals(OperationType.READ)) {
                blockRead(transaction, site.getLockManager(), variable, operation);
            }
        }
        //TODO: Add Deadlock detection
    }

    private void addWaitingSite(Transaction transaction, Site site) {
        List<Site> waitingSitesForTransaction = waitingSites.getOrDefault(transaction.getTransactionId(), new ArrayList<>());
        waitingSitesForTransaction.add(site);
        waitingSites.put(transaction.getTransactionId(), waitingSitesForTransaction);
        FileUtils.log("Transaction " + transaction.getTransactionId() + " waiting for Site " + site.getSiteId());
    }

    private void blockRead(Transaction transaction, LockManager lockManager, Integer variable, Operation operation) {
        addWaitsForWriteLock(transaction, lockManager, variable);
        addWaitingOperation(variable, operation);
    }

    private void addWaitsForWriteLock(Transaction transaction, LockManager lockManager, Integer variable) {
        Optional<Transaction> waitsFor = lockManager.waitsForWriteTransaction(variable);
        waitsFor.ifPresent(value -> addToWaitingTransactions(value, transaction));
    }

    private void addToWaitingTransactions(Transaction waitsFor, Transaction transaction) {
        if (waitsFor.getTransactionId().equals(transaction.getTransactionId())) return;
        List<Transaction> waitingList = waitingTransactions.getOrDefault(waitsFor.getTransactionId(), new ArrayList<>());
        waitingList.add(transaction);
        waitingTransactions.put(waitsFor.getTransactionId(), waitingList);
    }

    private void addWaitingOperation(Integer variable, Operation operation) {
        List<Operation> operations = waitingOperations.getOrDefault(variable, new ArrayList<>());
        operations.add(operation);
        waitingOperations.put(variable, operations);
    }

    private void wakeupTransactionsWaitingForSite(Site site) {
        waitingSites.forEach((transactionId, waitingSites) -> {
            sites.remove(site);
            if (waitingSites.isEmpty()) {
                FileUtils.log("Transaction: " + transactionId + " woken up since site: " + site.getSiteId() + " is up!");
                Transaction transaction = getTransactionOrThrowException(transactionId);
                Operation currentOperation = transaction.getCurrentOperation();
                if (OperationType.READ.equals(currentOperation.getOperationType())) {
                    read(currentOperation);
                } else if (OperationType.WRITE.equals(currentOperation.getOperationType())) {
                    write(currentOperation);
                }
            }
        });

    }
}
