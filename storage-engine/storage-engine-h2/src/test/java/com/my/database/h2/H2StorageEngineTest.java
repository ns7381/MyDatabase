package com.my.database.h2;

import com.my.database.api.Cell;
import com.my.database.api.Row;
import com.my.database.api.RowSet;
import com.my.database.api.operator.FilterOperator;
import com.my.database.api.operator.ProjectionOperator;
import org.junit.Test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class H2StorageEngineTest {
    @Test
    public void testCreateTable() throws SQLException {
        H2StorageEngine h2StorageEngine = new H2StorageEngine();
        Cell id = new Cell("id");
        Cell name = new Cell("name");
        List<Cell> cells = new ArrayList<>();
        cells.add(id);
        cells.add(name);
        Row row = new Row(cells);
        h2StorageEngine.createTable("user", row);
    }
    @Test
    public void testInsert() throws SQLException {
        H2StorageEngine h2StorageEngine = new H2StorageEngine();
        h2StorageEngine.insert("user", 1, "nathan");
    }
    @Test
    public void testSelect() throws SQLException {
        H2StorageEngine h2StorageEngine = new H2StorageEngine();
//        ProjectionOperator projectionOperator = new ProjectionOperator(row -> row);
        FilterOperator filterOperator = new FilterOperator(row -> row.getCells().get(0).getVal().equals("1"));
        RowSet user = h2StorageEngine.select("user", Collections.emptyList());
//        RowSet user = h2StorageEngine.select("user", Collections.singletonList(filterOperator));
        System.out.println(user.getRows());
    }
}
