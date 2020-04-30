package com.my.database.lsm.table;

import com.my.database.lsm.exception.StorageEngineException;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang.StringUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;

@Setter
@Getter
public class Table {
    private String name;
    private Map<String, MemTable> memTables = new HashMap<>();
    private PriorityQueue<ComparableTimed<Row>> cache = new PriorityQueue<>(Config.rowCacheCapacity);

    public Table(String name, String[] columnNames) {
        this.name = name;
        for (String columnName : columnNames) {
            this.memTables.put(columnName, new MemTable(name, columnName, columnNames));
        }
    }

    public void put(Row row) {
        if (StringUtils.isBlank(row.getKey())) {
            throw new StorageEngineException("Row key is null");
        }
        // 1. TODO: save row to wal log
        this.cache.add(new ComparableTimed<>(row));
        //2. save (rowkey, val) to memory
        row.getCols().forEach((k, v) -> {
            try {
                this.memTables.get(k).put(row.getKey(), v);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    public Row get(String rowKey) throws InterruptedException {
        // Check the recently accessed row cache
        for (ComparableTimed<Row> timed : this.cache) {
            if (timed.getRow().getKey().equals(rowKey)) {
                return timed.getRow();
            }
        }

        Map<String, String> columnValues = new HashMap<>();
        for (String columnName : this.memTables.keySet()) {
            columnValues.put(columnName, this.memTables.get(columnName).get(rowKey));
        }
        for (String columnValue : columnValues.values()) {
            // As long as one column value is not null, we know this row is not deleted
            if (columnValue != null) {
                Row newRow = new Row(rowKey, columnValues);
                this.cache.add(new ComparableTimed<>(newRow));
                return newRow;
            }
        }
        return null;
    }
}
