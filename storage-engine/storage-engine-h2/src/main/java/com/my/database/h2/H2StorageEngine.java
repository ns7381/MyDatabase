package com.my.database.h2;

import com.my.database.api.Cell;
import com.my.database.api.Row;
import com.my.database.api.RowSet;
import com.my.database.api.StorageEngine;
import com.my.database.api.operator.FilterOperator;
import com.my.database.api.operator.ProjectionOperator;
import com.my.database.api.operator.UnaryOperator;
import org.h2.tools.Csv;
import org.h2.tools.SimpleResultSet;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class H2StorageEngine implements StorageEngine {
    @Override
    public void init() throws Exception {

    }

    @Override
    public void createTable(String tableName, Row row) throws SQLException {
        SimpleResultSet rs = new SimpleResultSet();
        row.getCells().forEach(cell -> {
            rs.addColumn(cell.getName(), Types.VARCHAR, 255, 0);
        });
        rs.addRow(1, "test");
        rs.addRow(2, "test2");
        new Csv().write("data/" + tableName + ".csv", rs, null);
    }

    @Override
    public void insert(String tableName, Object... values) throws SQLException {
        SimpleResultSet rs = (SimpleResultSet) new Csv().read("data/" + tableName + ".csv", null, null);
        rs.addRow(values);
        new Csv().write("data/" + tableName + ".csv", rs, null);
    }

    @Override
    public RowSet select(String tableName, List<UnaryOperator> operators) throws SQLException {
        ResultSet rs = new Csv().read("data/" + tableName + ".csv", null, null);
        ResultSetMetaData meta = rs.getMetaData();
        AtomicReference<RowSet> rowSet = new AtomicReference<>(new RowSet());

        while (rs.next()) {
            List<Cell> cols = new ArrayList<Cell>();
            for (int i = 0; i < meta.getColumnCount(); i++) {
                Cell cell = new Cell(meta.getColumnTypeName(i + 1), meta.getColumnLabel(i + 1), rs.getString(i + 1));
                cols.add(cell);
            }
            rowSet.get().getRows().add(new Row(cols));
        }
        rs.close();
        operators.forEach(unaryOperator -> {
            rowSet.set(unaryOperator.execute(rowSet.get()));
        });
        return rowSet.get();
    }
}
