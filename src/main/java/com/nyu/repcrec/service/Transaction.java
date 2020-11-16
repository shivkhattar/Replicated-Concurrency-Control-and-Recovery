package com.nyu.repcrec.service;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;

@AllArgsConstructor
@Data
@EqualsAndHashCode
public class Transaction {

    private Integer transactionId;
    private Integer timestamp;
    private Operation currentOperation;
    private boolean isReadOnly;
    private TransactionStatus transactionStatus;
}
