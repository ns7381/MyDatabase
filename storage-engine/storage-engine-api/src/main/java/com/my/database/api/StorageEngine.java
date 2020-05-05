package com.my.database.api;

import com.my.database.api.operator.UnaryOperator;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public interface StorageEngine {

    void init() throws Exception;

    void createTable(String tableName, Row row) throws Exception;

//    void insert(String tableName, Object... values) throws Exception;

    void insert(String tableName, Map<String, Object> values) throws Exception;

    RowSet select(String tableName, List<UnaryOperator> operators) throws Exception;
}
