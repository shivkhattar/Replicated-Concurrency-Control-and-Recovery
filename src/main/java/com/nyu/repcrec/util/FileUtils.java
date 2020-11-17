package com.nyu.repcrec.util;

import com.nyu.repcrec.service.Operation;
import org.apache.commons.lang3.StringUtils;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

import static com.nyu.repcrec.service.OperationType.*;

public class FileUtils {

    private static String outputFile = "src/main/resources/output/output-0";

    public static List<Operation> parseFile(String inputFile) throws IOException {
        if (StringUtils.isEmpty(inputFile)) {
            throw new IOException("Input path not provided");
        }
        String COMMENT = "//";

        List<String> allLines = Files.readAllLines(Paths.get(inputFile));
        return allLines.stream().filter(line -> !line.isEmpty() && !line.startsWith(COMMENT))
                .map(FileUtils::getOperation).collect(Collectors.toList());

    }

    private static Operation getOperation(String operationString) {
        String[] components = getTrimmedComponents(operationString.trim().split("[(),]"));
        String operationType = components[0];
        Operation operation;
        if (BEGIN.getValue().equalsIgnoreCase(operationType) && components.length == 2) {
            operation = new Operation(BEGIN);
            operation.setTransactionId(getId(components[1]));
        } else if (END.getValue().equalsIgnoreCase(operationType) && components.length == 2) {
            operation = new Operation(END);
            operation.setTransactionId(getId(components[1]));
        } else if (BEGINRO.getValue().equalsIgnoreCase(operationType) && components.length == 2) {
            operation = new Operation(BEGINRO);
            operation.setTransactionId(getId(components[1]));
        } else if (DUMP.getValue().equalsIgnoreCase(operationType) && components.length == 1) {
            operation = new Operation(DUMP);
        } else if (RECOVER.getValue().equalsIgnoreCase(operationType) && components.length == 2) {
            operation = new Operation(RECOVER);
            operation.setSiteId(getIntegerFromString(components[1]));
        } else if (FAIL.getValue().equalsIgnoreCase(operationType) && components.length == 2) {
            operation = new Operation(FAIL);
            operation.setSiteId(getIntegerFromString(components[1]));
        } else if (READ.getValue().equalsIgnoreCase(operationType) && components.length == 3) {
            operation = new Operation(READ);
            operation.setTransactionId(getId(components[1]));
            operation.setVariable(getId(components[2]));
        } else if (WRITE.getValue().equalsIgnoreCase(operationType) && components.length == 4) {
            operation = new Operation(WRITE);
            operation.setTransactionId(getId(components[1]));
            operation.setVariable(getId(components[2]));
            operation.setWriteValue(getIntegerFromString(components[3]));
        } else {
            log("Unsupported Operation: " + operationType);
            throw new UnsupportedOperationException("This operation is not supported");
        }
        return operation;
    }

    private static String[] getTrimmedComponents(String[] components) {
        for (int i = 0; i < components.length; i++) {
            components[i] = components[i].trim();
        }
        return components;
    }

    private static Integer getId(String transactionString) {
        return getIntegerFromString(transactionString.substring(1));
    }

    private static Integer getIntegerFromString(String value) {
        return Integer.valueOf(value);
    }

    public static void createOutputFile(String outputPath) throws IOException {
        if (StringUtils.isEmpty(outputPath)) {
            throw new IOException("Output path not provided");
        }
        File file = new File(outputPath);
        outputFile = outputPath;
        if (Files.exists(Paths.get(outputFile))) file.delete();
    }

    public static void log(String message) {
        System.out.println(message);
        try (FileWriter fw = new FileWriter(outputFile, true);
             BufferedWriter bw = new BufferedWriter(fw);
             PrintWriter out = new PrintWriter(bw)) {
            out.println(message);
        } catch (IOException ioException) {
            System.err.println("Exception while writing into file: " + outputFile + "Exception: ");
            ioException.printStackTrace();
        }
    }
}
