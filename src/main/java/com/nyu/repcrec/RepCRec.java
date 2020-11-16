package com.nyu.repcrec;

import com.nyu.repcrec.service.Operation;
import com.nyu.repcrec.service.TransactionManager;

import java.util.List;

public class RepCRec {

    private List<Operation> operations;
    private TransactionManager transactionManager;

    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Please provide an input!");
            System.exit(0);
        }
        System.out.println((args[0]));

    }
}
