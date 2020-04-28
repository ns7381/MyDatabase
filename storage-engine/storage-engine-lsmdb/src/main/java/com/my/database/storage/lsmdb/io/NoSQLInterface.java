package com.my.database.storage.lsmdb.io;

import com.my.database.storage.lsmdb.utils.Row;

import java.io.IOException;
import java.util.Collections;

/**
 * The class that handles all the management of the database.
 * It works as the interface of our Database implementation.
 */
public final class NoSQLInterface {
    private NoSQLInterface() {
    }

    /**
     * Create an application which user can add tables to it
     *
     * @param applicationName the name of the application
     * @return the application created
     */
    public static Application createApplication(String applicationName) {
        return new Application(applicationName);
    }

    /**
     * Create a table
     *
     * @param tableName   String of table name
     * @param columnNames Array of column names
     * @return the table object
     */
    public static Table createTable(String tableName, String[] columnNames) {
        return new Table(tableName, columnNames);
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        Application app = createApplication("app");

        Table table = createTable("t1", new String[]{"c1"});
        table.insert(new Row("1", Collections.singletonMap("c1", "v1")));
        Row row = table.selectRowKey("1");

        table.flush();
    }
}
