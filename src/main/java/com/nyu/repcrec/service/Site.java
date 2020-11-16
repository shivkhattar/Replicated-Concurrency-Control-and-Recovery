package com.nyu.repcrec;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Data
@EqualsAndHashCode
public class Site {

    private Integer siteId;
    private boolean isUp;
    private LockManager lockManager;
    private DataManager dataManager;
}
