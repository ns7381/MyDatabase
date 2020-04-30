package com.my.database.lsm.table;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
@AllArgsConstructor
public class Descriptor {
    private final String table;
    private final String ns;
    private final String cf;
    private final String[] cols;
}
