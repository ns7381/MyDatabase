package com.my.database.bplus;

import com.my.database.api.Cell;
import com.my.database.api.Row;
import com.my.database.api.RowSet;
import com.my.database.api.StorageEngine;
import com.my.database.api.operator.UnaryOperator;
import com.my.database.bplus.db.Database;
import com.my.database.bplus.db.Tuple;

import java.util.*;

public class BplusStorageEngine implements StorageEngine {
    private Database db;
    private volatile static BplusStorageEngine singleton;

    private BplusStorageEngine() {
        this.db = new Database();
        try {
            this.db.init();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static BplusStorageEngine getBplusStorageEngine() {
        if (singleton == null) {
            synchronized (BplusStorageEngine.class) {
                if (singleton == null) {
                    singleton = new BplusStorageEngine();
                }
            }
        }
        return singleton;

    }

    @Override
    public void init() throws Exception {
        db.init();
    }

    @Override
    public void createTable(String tableName, Row row) throws Exception {
        Hashtable<String, String> colType = new Hashtable<String, String>();
        final String[] key = new String[1];
        row.getCells().forEach(cell -> {
            colType.put(cell.getName(), cell.getType());
            if (cell.isPrimary()) {
                key[0] = cell.getName();
            }
        });
        db.createTable(tableName, colType, new Hashtable<String, String>(), key[0]);
    }

    @Override
    public void insert(String tableName, Map<String, Object> values) throws Exception {
        Hashtable<String, Object> col = new Hashtable<String, Object>();
        values.forEach(col::put);
        db.insertIntoTable(tableName, col);
    }

    @Override
    public RowSet select(String tableName, List<UnaryOperator> operators) throws Exception {
        RowSet rowSet = new RowSet();

        Iterator<Tuple> rows = db.selectFromTable(tableName, new Hashtable<>(), "and");
        while (rows.hasNext()) {
            Tuple next = rows.next();
            List<Cell> cols = new ArrayList<Cell>();
            Enumeration<String> keys = next.getData().keys();
            while (keys.hasMoreElements()) {
                String name = keys.nextElement();
                cols.add(new Cell(name, next.getData().get(name)));
            }
            rowSet.getRows().add(new Row(cols));
        }
        for (int i = 0; i < operators.size(); i++) {
            rowSet = operators.get(i).execute(rowSet);
        }
        return rowSet;
    }
}
