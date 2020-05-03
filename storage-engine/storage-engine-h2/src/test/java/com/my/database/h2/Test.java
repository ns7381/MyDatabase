package com.my.database.h2;

import org.h2.tools.Csv;
import org.h2.tools.SimpleResultSet;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

public class Test {
    @org.junit.Test
    public void testCreateTable() throws SQLException {
        SimpleResultSet rs = new SimpleResultSet();
        rs.addColumn("NAME", Types.VARCHAR, 255, 0);
        rs.addColumn("EMAIL", Types.VARCHAR, 255, 0);
//        rs.addRow("Bob Meier", "bob.meier@abcde.abc");
//        rs.addRow("John Jones", "john.jones@abcde.abc");
        new Csv().write("data/test.csv", rs, null);
    }

    @org.junit.Test
    public void testRead() throws SQLException {
        ResultSet rs = new Csv().read("data/test.csv", null, null);
        ResultSetMetaData meta = rs.getMetaData();
        while (rs.next()) {
            for (int i = 0; i < meta.getColumnCount(); i++) {

                System.out.println(
                        meta.getColumnLabel(i + 1) + ": " +
                                rs.getString(i + 1));
            }
            System.out.println();
        }
        rs.close();
    }
}
