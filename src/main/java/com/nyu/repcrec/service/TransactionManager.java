package com.nyu.repcrec.service;

import com.nyu.repcrec.util.Constants;
import com.nyu.repcrec.util.FileUtils;
import lombok.EqualsAndHashCode;

import java.util.*;

import static com.nyu.repcrec.service.OperationType.READ;
import static com.nyu.repcrec.service.OperationType.WRITE;
import static com.nyu.repcrec.service.TransactionStatus.*;

@EqualsAndHashCode
public class TransactionManager {

    private Integer currentTimestamp;
    private Transactions transactions;
    private List<Site> sites;
    //transactionId -> List<Transaction>
    private Map<Integer, List<Transaction>> waitsForGraph;
    //transactionId -> List<Site>
    private Map<Integer, List<Site>> waitingSites;
    //variable -> List<Operation>
    private Map<Integer, LinkedList<Operation>> waitingOperations;
    private Transaction youngestTransaction;

    private static final Integer MIN_SITE_ID = 1;
    private static final Integer MAX_SITE_ID = 10;
    private static final Integer MIN_VARIABLE = 1;
    private static final Integer MAX_VARIABLE = 20;

    public TransactionManager() {
        currentTimestamp = 0;
        transactions = new Transactions();
        sites = new ArrayList<>();
        waitsForGraph = new HashMap<>();
        waitingSites = new HashMap<>();
        waitingOperations = new HashMap<>();

        //Create 11 new sites, first one being the dummy site
        for (int i = 0; i <= MAX_SITE_ID; i++) sites.add(new Site(i));

        //create new variables and place them at the correct site
        for (int variable = MIN_VARIABLE; variable <= MAX_VARIABLE; variable++) {
            if (isOddVariable(variable))
                addVariableAtSiteId(variable, getSiteIdForOddVariable(variable));
            else
                addVariableAtAllSites(variable);
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
        return 1 + (variable % 10);
    }

    public void executeOperation(Operation operation) {
        currentTimestamp++;
        switch (operation.getOperationType()) {
            case BEGIN:
                beginTransaction(operation, false);
                break;
            case BEGINRO:
                beginTransaction(operation, true);
                break;
            case END:
                endTransaction(operation.getTransactionId());
                break;
            case READ:
                read(operation, true);
                break;
            case WRITE:
                write(operation, true);
                break;
            case DUMP:
                dump();
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

    protected void beginTransaction(Operation operation, boolean isReadOnly) {
        if (getTransaction(operation.getTransactionId()).isPresent()) {
            throw new RepCRecException("Transaction with id: " + operation.getTransactionId() + " already exists!");
        }
        FileUtils.log("Begin " + (isReadOnly ? "RO " : "") + "T" + operation.getTransactionId() + " at time: " + currentTimestamp);
        transactions.put(new Transaction(operation.getTransactionId(), currentTimestamp, operation, isReadOnly, ACTIVE));
    }

    protected void endTransaction(Integer transactionId) {
        Transaction transaction = getTransactionOrThrowException(transactionId);
        Set<Integer> variablesHeldByTransaction = new LinkedHashSet<>();

        if (COMMITTED.equals(transaction.getTransactionStatus()) || ABORTED.equals(transaction.getTransactionStatus())) {
            FileUtils.log(String.format("T%d is already %s", transactionId, transaction.getTransactionStatus().name().toLowerCase()));
            return;
        }

        transaction.setTransactionStatus(ACTIVE.equals(transaction.getTransactionStatus()) ? COMMITTED : ABORTED);
        FileUtils.log("T" + transactionId + " " + transaction.getTransactionStatus().name().toLowerCase());
        for (Integer siteId = MIN_SITE_ID; siteId <= MAX_SITE_ID; siteId++) {
            Site site = sites.get(siteId);
            variablesHeldByTransaction.addAll(site.getLockManager().getAllVariablesHeldByTransaction(transaction));
            if (COMMITTED.equals(transaction.getTransactionStatus())) {
                site.commitValues(transaction, currentTimestamp);
            } else {
                site.moveCurrentValuesBackToCommitted(transaction, transaction.getTimestamp());
            }
            site.getLockManager().eraseLocksForTransaction(transaction);
        }
        removeFromWaitingTransactions(transaction);
        removeFromWaitingOperations(transaction);
        sites.stream().skip(MIN_SITE_ID).forEach(this::wakeupTransactionsWaitingForSite);
        wakeupTransactionsWaitingForVariables(variablesHeldByTransaction);
    }

    private void removeFromWaitingTransactions(Transaction transaction) {
        waitsForGraph.forEach((transactionId, waiting) -> waiting.remove(transaction));
        waitsForGraph.entrySet().removeIf(entry -> transaction.getTransactionId().equals(entry.getKey()) || entry.getValue().isEmpty());
    }

    private void removeFromWaitingOperations(Transaction transaction) {
        waitingOperations.forEach((variable, operations) -> operations.removeIf(operation -> operation.getTransactionId().equals(transaction.getTransactionId())));
        waitingOperations.entrySet().removeIf(entry -> entry.getValue().isEmpty());
    }

    private Transaction getTransactionOrThrowException(Integer transactionId) {
        return getTransaction(transactionId).orElseThrow(() -> new RepCRecException("Transaction with id:" + transactionId + " not found"));
    }

    protected Optional<Transaction> getTransaction(Integer transactionId) {
        return transactions.get(transactionId);
    }

    protected void read(Operation operation, boolean isNewRead) {
        Transaction transaction = getTransactionOrThrowException(operation.getTransactionId());
        if (COMMITTED.equals(transaction.getTransactionStatus()) || ABORTED.equals(transaction.getTransactionStatus())) {
            FileUtils.log("Finished T" + transaction.getTransactionId() + " trying to read. Ignoring.");
            return;
        }
        Integer variable = operation.getVariable();
        transaction.setCurrentOperation(operation);
        if (transaction.isReadOnly()) {
            readForReadOnlyTransaction(transaction, variable);
        } else {
            read(transaction, variable, operation, isNewRead);
        }
    }

    protected void readForReadOnlyTransaction(Transaction transaction, Integer variable) {
        if (isOddVariable(variable)) {
            readOnlyTransactionOddVariable(transaction, variable);
        } else {
            readOnlyTransactionEvenVariable(transaction, variable);
        }
    }

    private void read(Transaction transaction, Integer variable, Operation operation, boolean isNewRead) {
        if (isOddVariable(variable)) {
            readOddVariable(transaction, variable, operation, isNewRead);
        } else {
            readEvenVariable(transaction, variable, operation, isNewRead);
        }
    }

    private void readOnlyTransactionOddVariable(Transaction transaction, Integer variable) {
        Site site = sites.get(getSiteIdForOddVariable(variable));
        if (site.isReadAllowed(variable)) {
            readVariable(transaction, variable, site);
        } else {
            addWaitingSite(transaction, site);
        }
    }

    private void readOnlyTransactionEvenVariable(Transaction transaction, Integer variable) {
        for (int i = MIN_SITE_ID; i <= MAX_SITE_ID; i++) {
            Site site = sites.get(i);
            if (site.isReadAllowed(variable)) {
                readVariable(transaction, variable, site);
                return;
            }
        }
        sites.stream().skip(MIN_SITE_ID).forEach(site -> addWaitingSite(transaction, site));
    }

    private boolean canRead(Transaction transaction, Integer variable) {
        return isOddVariable(variable) ? canReadOddVariable(transaction, variable) : canReadEvenVariable(transaction, variable);
    }

    private boolean canReadOddVariable(Transaction transaction, Integer variable) {
        Site site = sites.get(getSiteIdForOddVariable(variable));
        return site.isReadAllowed(variable) && site.getLockManager().canRead(transaction, variable);
    }

    private boolean canReadEvenVariable(Transaction transaction, Integer variable) {
        for (int i = MIN_SITE_ID; i <= MAX_SITE_ID; i++) {
            Site site = sites.get(i);
            if (site.isReadAllowed(variable) && site.getLockManager().canRead(transaction, variable)) {
                return true;
            }
        }
        return false;
    }

    private void readOddVariable(Transaction transaction, Integer variable, Operation operation, boolean isNewRead) {
        Site site = sites.get(getSiteIdForOddVariable(variable));
        if (canReadOddVariable(transaction, variable)) {
            if (checkWriteStarvation(transaction, variable, operation, isNewRead)) {
                readVariable(transaction, variable, site);
            }
        } else {
            block(transaction, site, variable, operation);
        }
    }

    private void readEvenVariable(Transaction transaction, Integer variable, Operation operation, boolean isNewRead) {
        if (!canReadEvenVariable(transaction, variable)) {
            sites.stream().skip(MIN_SITE_ID).forEach(site -> block(transaction, site, variable, operation));
            return;
        }
        for (int i = MIN_SITE_ID; i <= MAX_SITE_ID; i++) {
            Site site = sites.get(i);
            if (site.isReadAllowed(variable) && site.getLockManager().canRead(transaction, variable)) {
                if (checkWriteStarvation(transaction, variable, operation, isNewRead)) {
                    readVariable(transaction, variable, site);
                }
                return;
            }
        }
    }

    protected boolean checkWriteStarvation(Transaction transaction, Integer variable, Operation operation, boolean isNewReadOrWrite) {
        if (!isNewReadOrWrite || !hasWriteWaiting(operation, variable)) return true;
        addWaitsForWaitingWrite(transaction, operation, variable);
        addWaitingOperation(variable, operation);
        detectDeadlock();
        return false;
    }

    private boolean hasWriteWaiting(Operation operation, Integer variable) {
        LinkedList<Operation> operationsInLine = waitingOperations.getOrDefault(variable, new LinkedList<>());
        return operationsInLine.stream().anyMatch(waitingOperation -> !waitingOperation.equals(operation) && WRITE.equals(waitingOperation.getOperationType()));
    }

    private void addWaitsForWaitingWrite(Transaction transaction, Operation operation, Integer variable) {
        LinkedList<Operation> operationsInLine = waitingOperations.getOrDefault(variable, new LinkedList<>());
        Optional<Operation> waitsForOperation = operationsInLine.stream().filter(waitingOperation -> !waitingOperation.equals(operation) && WRITE.equals(waitingOperation.getOperationType())).findFirst();
        waitsForOperation.ifPresent(waitsFor -> addToWaitingTransactions(getTransactionOrThrowException(waitsFor.getTransactionId()), transaction, null, variable));
    }

    private void readVariable(Transaction transaction, Integer variable, Site site) {
        Integer value = site.readValue(transaction, variable);
        FileUtils.log("T" + transaction.getTransactionId() + ": read(x" + variable + ")=" + value + " at site" + site.getSiteId());
    }

    protected void write(Operation operation, boolean isNewWrite) {
        Transaction transaction = getTransactionOrThrowException(operation.getTransactionId());
        if (COMMITTED.equals(transaction.getTransactionStatus()) || ABORTED.equals(transaction.getTransactionStatus())) {
            FileUtils.log("Finished T " + transaction.getTransactionId() + " trying to write. Ignoring.");
            return;
        }
        Integer variable = operation.getVariable();
        Integer writeValue = operation.getWriteValue();

        transaction.setCurrentOperation(operation);
        if (isOddVariable(variable)) writeOddVariable(transaction, variable, writeValue, operation, isNewWrite);
        else writeEvenVariable(transaction, variable, writeValue, operation, isNewWrite);
    }

    private void writeOddVariable(Transaction transaction, Integer variable, Integer writeValue, Operation operation, boolean isNewWrite) {
        Site site = sites.get(getSiteIdForOddVariable(variable));
        if (canWriteOddVariable(transaction, variable)) {
            if (checkWriteStarvation(transaction, variable, operation, isNewWrite)) {
                writeVariable(transaction, variable, writeValue, site);
            }
        } else {
            block(transaction, site, variable, operation);
        }
    }

    private void writeEvenVariable(Transaction transaction, Integer variable, Integer writeValue, Operation operation, boolean isNewWrite) {
        if (canWriteEvenVariable(transaction, variable)) {
            if (checkWriteStarvation(transaction, variable, operation, isNewWrite)) {
                sites.stream().skip(MIN_SITE_ID).filter(Site::isUp).forEach(site -> writeVariable(transaction, variable, writeValue, site));
            }
        } else {
            if (allSitesDown()) {
                sites.stream().skip(MIN_SITE_ID).forEach(site -> addWaitingSite(transaction, site));
            } else {
                for (int i = MIN_SITE_ID; i <= MAX_SITE_ID; i++) {
                    Site site = sites.get(i);
                    if (site.isUp() && !site.getLockManager().canWrite(transaction, variable)) {
                        block(transaction, site, variable, operation);
                        return;
                    }
                }
            }
        }
    }

    private boolean canWrite(Transaction transaction, Integer variable) {
        return isOddVariable(variable) ? canWriteOddVariable(transaction, variable) : canWriteEvenVariable(transaction, variable);
    }

    private boolean canWriteOddVariable(Transaction transaction, Integer variable) {
        Site site = sites.get(getSiteIdForOddVariable(variable));
        return site.isUp() && site.getLockManager().canWrite(transaction, variable);
    }

    private boolean canWriteEvenVariable(Transaction transaction, Integer variable) {
        if (allSitesDown()) return false;
        for (int i = MIN_SITE_ID; i <= MAX_SITE_ID; i++) {
            Site site = sites.get(i);
            if (site.isUp() && !site.getLockManager().canWrite(transaction, variable)) {
                return false;
            }
        }
        return true;
    }

    private boolean allSitesDown() {
        return sites.stream().skip(MIN_SITE_ID).noneMatch(Site::isUp);
    }

    private void writeVariable(Transaction transaction, Integer variable, Integer writeValue, Site site) {
        site.writeValue(transaction, variable, writeValue);
        FileUtils.log("T" + transaction.getTransactionId() + ": write(x" + variable + ")=" + writeValue + " at site" + site.getSiteId());
    }

    protected void block(Transaction transaction, Site site, Integer variable, Operation operation) {
        if (!site.isUp() || (operation.getOperationType().equals(READ) && !site.isReadAllowed(variable))) {
            addWaitingSite(transaction, site);
            return;
        }
        if (operation.getOperationType().equals(READ)) {
            blockRead(transaction, site, variable, operation);
        } else if (operation.getOperationType().equals(WRITE)) {
            blockWrite(transaction, site, variable, operation);
        }
        detectDeadlock();
    }

    private void detectDeadlock() {
        Optional<Transaction> abortTransaction = findYoungestDeadlockedTransaction();
        while (abortTransaction.isPresent()) {
            abortTransaction.get().setTransactionStatus(ABORT);
            endTransaction(abortTransaction.get().getTransactionId());
            abortTransaction = findYoungestDeadlockedTransaction();
        }
    }


    protected void addWaitingSite(Transaction transaction, Site site) {
        if (Objects.nonNull(site)) {
            List<Site> waitingSitesForTransaction = waitingSites.getOrDefault(transaction.getTransactionId(), new ArrayList<>());
            waitingSitesForTransaction.add(site);
            waitingSites.put(transaction.getTransactionId(), waitingSitesForTransaction);
            FileUtils.log("T" + transaction.getTransactionId() + " waits for Site" + site.getSiteId());
        }
    }

    private void blockRead(Transaction transaction, Site site, Integer variable, Operation operation) {
        addWaitsForWriteLock(transaction, site, variable);
        addWaitingOperation(variable, operation);
    }

    private void blockWrite(Transaction transaction, Site site, Integer variable, Operation operation) {
        addWaitsForWriteLock(transaction, site, variable);
        addWaitsForReadLock(transaction, site, variable);
        addWaitingOperation(variable, operation);
    }

    private void addWaitsForWriteLock(Transaction transaction, Site site, Integer variable) {
        Optional<Transaction> waitsFor = site.getLockManager().waitsForWriteTransaction(variable);
        waitsFor.ifPresent(waitsForTransaction -> addToWaitingTransactions(waitsForTransaction, transaction, site, variable));
    }

    private void addWaitsForReadLock(Transaction transaction, Site site, Integer variable) {
        List<Transaction> waitsFor = site.getLockManager().waitsForReadTransaction(variable);
        waitsFor.forEach(waitsForTransaction -> addToWaitingTransactions(waitsForTransaction, transaction, site, variable));
    }

    private void addToWaitingTransactions(Transaction waitsFor, Transaction transaction, Site site, Integer
            variable) {
        if (waitsFor.getTransactionId().equals(transaction.getTransactionId())) return;
        List<Transaction> waitingList = waitsForGraph.getOrDefault(waitsFor.getTransactionId(), new ArrayList<>());
        if (!waitingList.contains(transaction)) {
            waitingList.add(transaction);
            waitsForGraph.put(waitsFor.getTransactionId(), waitingList);
            FileUtils.log("T" + transaction.getTransactionId() + " waits for T" + waitsFor.getTransactionId() + " for x" + variable
                    + (Objects.isNull(site) ? " because of write waiting" : " at site" + site.getSiteId()));
        }
    }

    private void addWaitingOperation(Integer variable, Operation operation) {
        if (Objects.nonNull(operation)) {
            LinkedList<Operation> operations = waitingOperations.getOrDefault(variable, new LinkedList<>());
            operations.add(operation);
            waitingOperations.put(variable, operations);
        }
    }

    protected void wakeupTransactionsWaitingForSite(Site site) {
        waitingSites.forEach((transactionId, sites) -> {
            Transaction transaction = getTransactionOrThrowException(transactionId);
            Operation currentOperation = transaction.getCurrentOperation();
            if (READ.equals(currentOperation.getOperationType())) {
                if (sites.contains(site) && site.isReadAllowed(currentOperation.getVariable())) {
                    FileUtils.log("T" + transactionId + " woken up since site" + site.getSiteId() + " is up!");
                    read(currentOperation, true);
                    sites.clear();
                }
            } else if (WRITE.equals(currentOperation.getOperationType())) {
                if (sites.contains(site)) {
                    FileUtils.log("T" + transactionId + " woken up since site" + site.getSiteId() + " is up!");
                    write(currentOperation, true);
                    sites.clear();
                }
            } else throw new RepCRecException("T" + transactionId + " should not be waiting for sites!");
        });
        waitingSites.entrySet().removeIf(entry -> entry.getValue().isEmpty());
    }

    protected void wakeupTransactionsWaitingForVariables(Set<Integer> variables) {
        variables.forEach(variable -> {
            if (waitingOperations.containsKey(variable)) {
                LinkedList<Operation> operations = waitingOperations.get(variable);
                boolean canReadOrWrite = true;
                while (!operations.isEmpty() && canReadOrWrite) {
                    Operation operation = operations.getFirst();
                    Transaction transaction = getTransactionOrThrowException(operation.getTransactionId());
                    if (READ.equals(operation.getOperationType())) {
                        if (canRead(transaction, variable)) {
                            read(operation, false);
                            operations.removeFirst();
                        } else canReadOrWrite = false;
                    } else if (WRITE.equals(operation.getOperationType())) {
                        if (canWrite(transaction, variable)) {
                            write(operation, false);
                            operations.removeFirst();
                        } else canReadOrWrite = false;
                    } else {
                        throw new RepCRecException(operation.getOperationType() + " for T" + operation.getTransactionId() + " should not be in the waiting operations!");
                    }
                }
                waitingOperations.entrySet().removeIf(entry -> entry.getValue().isEmpty());
            }
        });
    }

    protected void dump() {
        FileUtils.log(Constants.DUMP);
        sites.stream().skip(MIN_SITE_ID).forEach(site ->
        {
            TreeMap<Integer, DataValue> dataTreeMap = site.getDataManager().getData();
            StringBuilder sb = new StringBuilder(String.format("site %d -", site.getSiteId()));
            dataTreeMap.keySet().forEach(variableId -> {
                DataValue dataValue = dataTreeMap.get(variableId);
                sb.append(String.format(" x%d: %d,", variableId, dataValue.getLastCommittedValues().lastEntry().getValue()));
            });
            // Removing last comma
            sb.setLength(sb.length() - 1);
            FileUtils.log(sb.toString());
        });
        FileUtils.log(Constants.ASTERISK_LINE);
    }

    private Optional<Transaction> findYoungestDeadlockedTransaction() {
        youngestTransaction = null;
        for (Integer currentTransactionId : waitsForGraph.keySet()) {
            Transaction currentTransaction = getTransactionOrThrowException(currentTransactionId);
            Set<Integer> visited = new HashSet<>();

            if (detectCycle(visited, currentTransaction, currentTransaction)) {
                if (youngestTransaction != null && currentTransaction.getTimestamp() > youngestTransaction.getTimestamp()) {
                    youngestTransaction = currentTransaction;
                } else {
                    youngestTransaction = currentTransaction;
                }
            }
        }

        return Optional.ofNullable(youngestTransaction);
    }

    private boolean detectCycle(Set<Integer> visited, Transaction currentTransaction, Transaction startingTransaction) {

        visited.add(currentTransaction.getTransactionId());

        List<Transaction> connectedTransactions = waitsForGraph.getOrDefault(currentTransaction.getTransactionId(), new LinkedList<>());

        for (Transaction nextTransaction : connectedTransactions) {
            if (nextTransaction.equals(startingTransaction))
                return true;
            if (!visited.contains(nextTransaction.getTransactionId())) {
                if (detectCycle(visited, nextTransaction, startingTransaction)) {
                    return true;
                }
            }
        }

        return false;
    }
}