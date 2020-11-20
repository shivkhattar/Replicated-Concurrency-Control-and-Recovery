package com.nyu.repcrec;

import com.nyu.repcrec.service.Operation;
import com.nyu.repcrec.service.TransactionManager;
import com.nyu.repcrec.util.Constants;
import com.nyu.repcrec.util.FileUtils;

import java.util.List;

public class RepCRec {

    public static void main(String[] args) {
        try {
            for(int i = 0; i < args.length; i++){
                Integer inputFileNumber = Integer.parseInt(args[i]);
                FileUtils.log(String.format(Constants.INPUT_FILE, inputFileNumber));
                executeInputFile(inputFileNumber);
                FileUtils.log(Constants.ASTERISK_LINE);
            }

        } catch (Exception exception) {
            FileUtils.log(exception.getMessage());
            System.err.println("Exception occurred in execution. Exception: ");
            exception.printStackTrace();
        }

    }

    public static void executeInputFile(int fileNumber) throws Exception{
        FileUtils.createOutputFile(Constants.RESOURCE_DIR_PATH + "/output", "output-" + fileNumber);
        List<Operation> operations = FileUtils.parseFile(Constants.RESOURCE_DIR_PATH + "/input" + "/input-" + fileNumber);
        executeOperations(operations);

    }

    private static void executeOperations(List<Operation> operations) {
        TransactionManager transactionManager = new TransactionManager();
        operations.forEach(transactionManager::executeOperation);
    }
}
