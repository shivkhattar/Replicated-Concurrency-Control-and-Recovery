package com.nyu.repcrec;

import com.nyu.repcrec.service.Operation;
import com.nyu.repcrec.service.TransactionManager;
import com.nyu.repcrec.util.Constants;
import com.nyu.repcrec.util.FileUtils;

import java.util.List;

public class RepCRec {

    public static void main(String[] args) {
        try {
            FileUtils.createOutputFile(Constants.RESOURCE_DIR_PATH + "/output", "output-0");
            List<Operation> operations = FileUtils.parseFile(Constants.RESOURCE_DIR_PATH + "/input" + "/input-0");
            executeOperations(operations);
        } catch (Exception exception) {
            FileUtils.log(exception.getMessage());
            System.err.println("Exception occurred in execution. Exception: ");
            exception.printStackTrace();
        }
    }

    private static void executeOperations(List<Operation> operations) {
        TransactionManager transactionManager = new TransactionManager();
        operations.forEach(transactionManager::executeOperation);
    }
}
