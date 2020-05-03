package com.my.database.api;

import lombok.Getter;
import lombok.Setter;

import java.util.HashSet;
import java.util.Set;

@Getter
@Setter
public class RowSet {
    private Set<Row> rows;

    public RowSet() {
        this.rows = new HashSet<>();
    }
}
