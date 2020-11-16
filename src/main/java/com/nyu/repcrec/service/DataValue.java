package com.nyu.repcrec.service;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;

@AllArgsConstructor
@Data
@EqualsAndHashCode
public class DataValue {
    private Integer lastCommittedValue;
    private Integer currentValue;

}
