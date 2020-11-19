package com.nyu.repcrec.service;

public enum OperationType {

    BEGIN("begin"), END("end"), BEGINRO("beginRO"), READ("R"), WRITE("W"), DUMP("dump"), FAIL("fail"), RECOVER("recover");

    private String value;

    OperationType(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
