package com.nyu.repcrec;

public class RepCRec {
    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Please provide an input!");
            System.exit(0);
        }
        System.out.println((args[0]));

    }
}
