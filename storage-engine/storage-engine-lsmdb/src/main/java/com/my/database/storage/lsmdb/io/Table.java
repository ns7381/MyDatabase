package com.my.database.storage.lsmdb.io;

import com.my.database.storage.lsmdb.io.sstable.SSTable;
import com.my.database.storage.lsmdb.io.sstable.SSTableConfig;
import com.my.database.storage.lsmdb.io.sstable.blocks.Descriptor;
import com.my.database.storage.lsmdb.utils.Qualifier;
import com.my.database.storage.lsmdb.utils.Row;
import com.my.database.storage.lsmdb.utils.Timed;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.util.*;

public class Table implements Flushable, Closeable {
    private String tableName;
    private Map<String, SSTable> sstMap;
    private PriorityQueue<Timed<Row>> recentlyAccessedRows;

    /**
     * Constructor to create a table based on tableName and columnNames
     */
    Table(String tableName, String[] columnNames) {
        this.tableName = tableName;
        this.sstMap = new HashMap<>();
        SSTableConfig config = SSTableConfig.defaultConfig();
        this.recentlyAccessedRows = new PriorityQueue<>(config.getRowCacheCapacity());
        Descriptor desc = new Descriptor(tableName, "", "", columnNames);

        for (String columnName : columnNames) {
            this.sstMap.put(columnName, new SSTable(desc, columnName, config));
        }
    }

    /**
     * Insert a row into the table
     */
    public void insert(Row row) throws IOException {
        this.recentlyAccessedRows.add(new Timed<>(row));
        for (String columnName : this.sstMap.keySet()) {
            if (row.hasColumn(columnName)) {
                this.sstMap.get(columnName).put(row.getRowKey(), row.getColumnValue(columnName));

            }
        }
    }

    /**
     * Method to select row by rowKey
     */
    public Row selectRowKey(String rowKey) throws InterruptedException {
        // Check the recently accessed row cache
        for (Timed<Row> recentlyAccessedRow : this.recentlyAccessedRows) {
            Row rowToCheck = recentlyAccessedRow.get();
            if (rowToCheck.getRowKey().equals(rowKey)) {
                return rowToCheck;
            }
        }

        Map<String, String> columnValues = new HashMap<>();
        for (String columnName : this.sstMap.keySet()) {
            columnValues.put(columnName, this.sstMap.get(columnName).get(rowKey).orElse(null));
        }
        for (String columnValue : columnValues.values()) {
            // As long as one column value is not null, we know this row is not deleted
            if (columnValue != null) {
                Row newRow = new Row(rowKey, columnValues);
                this.recentlyAccessedRows.add(new Timed<>(newRow));
                return newRow;
            }
        }
        return null;
    }

    /**
     * Method to select rows by single column value
     */
    public List<Row> selectRowWithColumnValue(String columnName, String columnValue)
            throws IOException, InterruptedException {
        Qualifier q = new Qualifier("=", columnValue);
        List<Row> result = this.selectRowsWithQualifier(columnName, q);
        for (Row row : result) {
            this.recentlyAccessedRows.add(new Timed<>(row));
        }
        return result;
    }

    /**
     * Method to select rows with given column value range by comparator and target
     */
    public List<Row> selectRowsWithColumnRange(String columnName, String operator, String target) throws IOException, InterruptedException {
        Qualifier q = new Qualifier(operator, target);
        return this.selectRowsWithQualifier(columnName, q);
    }

    private List<Row> selectRowsWithQualifier(String columnName, Qualifier q) throws IOException, InterruptedException {
        Map<String, Row> result = new HashMap<>();

        // Create rows with only the selected column first
        Map<String, String> selected = this.sstMap.get(columnName).getColumnWithQualifier(q);
        for (Map.Entry<String, String> entry : selected.entrySet()) {
            String rowKey = entry.getKey();
            String colValue = entry.getValue();
            Row toAdd = new Row(rowKey, new HashMap<>());
            toAdd.addColumn(columnName, colValue);
            result.put(rowKey, toAdd);
        }

        // Request rows with rowKey in each column
        Qualifier qualifier = new Qualifier(result.keySet());
        for (String colName : this.sstMap.keySet()) {
            if (!colName.equals(columnName)) {
                Map<String, String> colValues = this.sstMap.get(colName).getColumnWithQualifier(qualifier);
                for (Map.Entry<String, String> entry : colValues.entrySet()) {
                    String rowKey = entry.getKey();
                    String colValue = entry.getValue();
                    if (result.containsKey(rowKey)) {
                        result.get(rowKey).addColumn(colName, colValue);
                    }
                }
            }
        }
        // Add rows to cache
        for (Row row : result.values()) {
            recentlyAccessedRows.add(new Timed<>(row));
        }

        return new ArrayList<>(result.values());
    }

    /**
     * Method to insert key-value pairs into one single column
     */
    public void insertColumnValues(String columnName, Map<String, String> rowKeyAndValues) throws IOException {
        for (Map.Entry<String, String> entry : rowKeyAndValues.entrySet()) {
            this.sstMap.get(columnName).put(entry.getKey(), entry.getValue());
        }
    }

    /**
     * Method to delete a row by rowKey
     */
    public void deleteRowKey(String rowKey) throws IOException {
        for (SSTable table : this.sstMap.values()) {
            table.put(rowKey, null);
        }
    }

    String getTableName() {
        return this.tableName;
    }

    /**
     * Method to insert a row
     */
    public void update(Row row) throws IOException {
        this.insert(row);
    }

    @Override
    public void flush() throws IOException {
        for (SSTable t : sstMap.values()) {
            t.flush();
        }
    }

    @Override
    public void close() throws IOException {
        for (SSTable t : sstMap.values()) {
            t.close();
        }
        sstMap.clear();
    }
}
