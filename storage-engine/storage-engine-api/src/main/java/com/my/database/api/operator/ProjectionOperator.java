package com.my.database.api.operator;

import com.my.database.api.Row;
import com.my.database.api.RowSet;
import lombok.AllArgsConstructor;

import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

@AllArgsConstructor
public class ProjectionOperator implements UnaryOperator {

    private Function<? super Row, ? extends Row> projection;

    @Override
    public RowSet execute(RowSet rowSet) {
        Set<Row> newRows = rowSet.getRows().stream().map(r->projection.apply(r)).collect(Collectors.toSet());
        rowSet.setRows(newRows);
        return rowSet;
    }
}
