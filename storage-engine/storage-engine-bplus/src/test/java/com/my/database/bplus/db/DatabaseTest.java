package com.my.database.bplus.db;

import org.junit.Test;

import java.util.Hashtable;
import java.util.Iterator;

public class DatabaseTest {
	@Test
	public void testCreateTable() throws Exception {
		Database database = new Database();
		database.init();
		Hashtable<String, String> colType = new Hashtable<String, String>();
		colType.put("id", "Integer");
		colType.put("name", "String");

		Hashtable<String, String> colRef = new Hashtable<String, String>();
		database.createTable("user", colType, colRef, "id");
	}
	@Test
	public void testInsert() throws Exception {
		Database database = new Database();
		database.init();
		Hashtable<String, Object> col = new Hashtable<String, Object>();
		col.put("id", Integer.valueOf("2"));
		col.put("name", "ming");
		database.insertIntoTable("user", col);
	}
	@Test
	public void testGet() throws Exception {
		Database database = new Database();
		database.init();
		Hashtable<String, Object> col = new Hashtable<String, Object>();
		col.put("id", Integer.valueOf("1"));
		Iterator<Tuple> tuples = database.selectFromTable("user", col, "and");
		while (tuples.hasNext()) {
			System.out.println(tuples.next());
		}
	}
	@Test
	public void testGetRange() throws Exception {
		Database database = new Database();
		database.init();
		Hashtable<String, Object> col = new Hashtable<String, Object>();
		Iterator<Tuple> tuples = database.selectFromTable("user", col, "and");
		while (tuples.hasNext()) {
			System.out.println(tuples.next());
		}
	}
}
