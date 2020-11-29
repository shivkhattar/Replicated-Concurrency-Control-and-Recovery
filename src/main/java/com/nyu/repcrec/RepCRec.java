package com.nyu.repcrec;

import com.nyu.repcrec.service.Operation;
import com.nyu.repcrec.service.TransactionManager;
import com.nyu.repcrec.util.Constants;
import com.nyu.repcrec.util.FileUtils;

import java.io.File;
import java.util.List;

public class RepCRec {

    public static void main(String[] args) {
        try {
            // Defaults to Input Resource directory if Directory not provided
            if(args.length == 0){
                args = new String[]{Constants.RESOURCE_DIR_PATH + "/input"};
            }
            final File folder = new File(args[0]);
            for (final File fileEntry : folder.listFiles()) {
                executeInputFile(fileEntry);
            }
        } catch (Exception exception) {
            FileUtils.log(exception.getMessage());
            System.err.println("Exception occurred in execution. Exception: ");
            exception.printStackTrace();
        }

    }

    public static void executeInputFile(File fileEntry) throws Exception{
        FileUtils.createOutputFile(Constants.RESOURCE_DIR_PATH + "/output", fileEntry.getName() + "-output");
        FileUtils.log(String.format(Constants.INPUT_FILE, fileEntry.getName()));
        List<Operation> operations = FileUtils.parseFile(fileEntry.getPath());
        executeOperations(operations);
        FileUtils.log(Constants.ASTERISK_LINE);
    }

    private static void executeOperations(List<Operation> operations) {
        TransactionManager transactionManager = new TransactionManager();
        operations.forEach(transactionManager::executeOperation);
    }
}
