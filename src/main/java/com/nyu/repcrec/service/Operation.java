package com.nyu.repcrec;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Data
@EqualsAndHashCode
public class Operation {
    private String operationType;
    private Integer transactionId;
    private Integer variable;
    private Integer siteId;
    private Integer writeValue;
}
