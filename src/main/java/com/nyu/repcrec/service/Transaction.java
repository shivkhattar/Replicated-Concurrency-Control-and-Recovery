package com.nyu.repcrec.service;

import lombok.*;

@AllArgsConstructor
@Data
@EqualsAndHashCode
public class Transaction {

    @Setter(AccessLevel.PRIVATE)
    private Integer transactionId;
    private Integer timestamp;
    private Operation currentOperation;
    private boolean isReadOnly;
    private TransactionStatus transactionStatus;
}
