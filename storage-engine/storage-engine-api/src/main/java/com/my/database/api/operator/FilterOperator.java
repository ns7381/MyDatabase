package com.my.database.api.operator;

import com.my.database.api.Row;
import com.my.database.api.RowSet;
import lombok.AllArgsConstructor;

import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@AllArgsConstructor
public class FilterOperator implements UnaryOperator {

    private Predicate<Row> predicate;

    @Override
    public RowSet execute(RowSet rowSet) {
        Set<Row> newRows = rowSet.getRows().stream().filter(r->predicate.test(r)).collect(Collectors.toSet());
        rowSet.setRows(newRows);
        return rowSet;
    }
}
