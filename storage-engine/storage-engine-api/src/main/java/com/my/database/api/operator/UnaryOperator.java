package com.my.database.api.operator;

import com.my.database.api.RowSet;

public interface UnaryOperator {
    RowSet execute(RowSet rowSet);
}
