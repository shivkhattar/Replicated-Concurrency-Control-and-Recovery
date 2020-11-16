package com.nyu.repcrec.service;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Data
@EqualsAndHashCode
public class DataValue {
    private Integer lastCommittedValue;
    private Integer currentValue;
}
