package com.nyu.repcrec;

import com.nyu.repcrec.service.Operation;
import com.nyu.repcrec.service.TransactionManager;
import com.nyu.repcrec.util.Constants;
import com.nyu.repcrec.util.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class RepCRec {

    public static void main(String[] args) {
        try {
            //Defaults to input resource directory if directory not provided
            if (args.length == 0) {
                args = new String[]{Constants.RESOURCE_DIR_PATH + "/input"};
            }
            final File folder = new File(args[0]);

            if (!folder.isDirectory()) {
                executeInputFile(folder);
            } else {
                for (final File fileEntry : Objects.requireNonNull(folder.listFiles())) {
                    executeInputFile(fileEntry);
                }
            }
        } catch (Exception exception) {
            FileUtils.log(exception.getMessage());
            System.err.println("Exception occurred in execution. Exception: ");
            exception.printStackTrace();
        }
    }

    public static void executeInputFile(File fileEntry) throws IOException {
        FileUtils.createOutputFile(Constants.RESOURCE_DIR_PATH + "/output", fileEntry.getName() + "-output");
        FileUtils.log(Constants.INPUT_FILE);
        List<Operation> operations = FileUtils.parseFile(fileEntry.getPath());
        executeOperations(operations);
        FileUtils.log(Constants.ASTERISK_LINE);
    }

    private static void executeOperations(List<Operation> operations) {
        TransactionManager transactionManager = new TransactionManager();
        operations.forEach(transactionManager::executeOperation);
    }
}
