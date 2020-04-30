package com.my.database.lsm.table;

import org.junit.Test;

import java.util.Collections;

public class TableTest {

    @Test
    public void testInsert() {
        Table table = new Table("test", new String[]{"c1", "c2"});
        for (int i = 0; i < 10000; i++) {
            Row row = new Row("row" + i, Collections.singletonMap("c1", "v" + i));
            table.put(row);
        }
    }

    @Test
    public void testGet() throws InterruptedException {
        Table table = new Table("test", new String[]{"c1", "c2"});
        Row row100 = table.get("row100");
        System.out.println(row100);
    }
}
