package com.nyu.repcrec.service;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode
public class Operation {
    private OperationType operationType;
    private Integer transactionId;
    private Integer variable;
    private Integer siteId;
    private Integer writeValue;

    public Operation(OperationType operationType) {
        this.operationType = operationType;
    }
}
