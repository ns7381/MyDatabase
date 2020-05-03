package com.my.database.api.operator;

import com.my.database.api.RowSet;
import lombok.AllArgsConstructor;

import java.util.stream.Collectors;

@AllArgsConstructor
public class LimitOperator implements UnaryOperator {
    private long limit;

    @Override
    public RowSet execute(RowSet rowSet) {
        rowSet.setRows(rowSet.getRows().stream().limit(limit).collect(Collectors.toSet()));
        return rowSet;
    }
}
