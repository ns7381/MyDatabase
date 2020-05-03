package com.my.database.api;

import com.my.database.api.operator.UnaryOperator;

import java.sql.SQLException;
import java.util.List;

public interface StorageEngine {

    void init() throws Exception;

    void createTable(String tableName, Row row) throws SQLException;

    void insert(String tableName, Object... values) throws SQLException;

    RowSet select(String tableName, List<UnaryOperator> operators) throws SQLException;
}
