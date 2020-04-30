package com.my.database.lsm.table;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Map;


@Getter
@AllArgsConstructor
public class Row {
    private String key;
    private Map<String, String> cols;
}
