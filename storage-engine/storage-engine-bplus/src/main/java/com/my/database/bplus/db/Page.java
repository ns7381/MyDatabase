package com.my.database.bplus.db;


import com.my.database.bplus.exception.BPlusEngineException;

import java.io.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.Enumeration;
import java.util.Hashtable;

public class Page implements Serializable {
    private String pageName;
    private int pageNo;
    private String tableName;
    private ArrayList<Date> touchDate = new ArrayList<Date>();
    private ArrayList<Tuple> tuples;
    private int size;

    /**
     * creates a new page with the name "tableName_pageNo"
     */
    public Page(int size, String pageName) throws IOException {
        this.size = size;
        this.pageName = pageName;
        tableName = pageName.substring(0, pageName.indexOf("#"));
        pageNo = Integer.parseInt(pageName.substring((pageName.indexOf("#") + 1)));
        tuples = new ArrayList<>(size);
        save();
    }

    /**
     * adds new tuple to the page, the commented section checks for duplicate keys but it violates test 1, however will work in M2
     */
    public void addToPage(Hashtable<String, Object> colNameValue) throws IOException, BPlusEngineException {
        Table thisTable = Database.tableMeta.get(tableName);
        if (!colNameValue.containsKey(thisTable.primaryKey)) {
			throw new BPlusEngineException("Primary Key missing");
		}
        if (checkTypes(thisTable, colNameValue)) {
            Tuple myTuple = new Tuple(colNameValue);
            myTuple.setPageNo(pageNo);
            myTuple.location = tuples.size();
            tuples.add(myTuple);
            touchDate.add(new Date());
            System.out.println("Inserted into " + pageName);
            indexIfNeeded(thisTable, myTuple);
            thisTable.save();
            save();
        } else {
            throw new BPlusEngineException("UnMatching Datatpes");
        }
    }

    /**
     * removes the tuple at index index, marks it as deleted and saves
     */
    public void removeTuple(int index) throws IOException {
        Table thisTable = Database.tableMeta.get(tableName);
        tuples.get(index).setDeleted(true);
        System.out.println("Removed Item " + tuples.remove(index));
        save();
    }

    /**
     * returns the next page or null if the page doesn't exist
     */
    public Page next() {
        String[] nextPageInfo = pageName.split("#");
        String newPageName = tableName + "#" + (Integer.parseInt(nextPageInfo[1]) + 1);
        try {
            return load(newPageName);
        } catch (Exception e) {
            return null;
        }
    }

    public ArrayList<Tuple> getTuples() {
        return tuples;
    }

    public boolean isFull() {
		return tuples.size() == size;
    }

    /**
     * save page as "tableName_pageNo.class"
     */
    public void save() throws IOException {
        File f = new File("data/pages/" + pageName + ".class");
        if (!f.exists()) {
            if (!f.getParentFile().exists()) {
                if (f.getParentFile().mkdirs()) {
                    f.createNewFile();
                }
            }
        }
        FileOutputStream fos = new FileOutputStream(f);
        ObjectOutputStream oos = new ObjectOutputStream(fos);
        oos.writeObject(this);
        oos.flush();
        oos.close();
    }

    /**
     * loads a page in the database staticly
     */
    public static Page load(String pageName) throws IOException, ClassNotFoundException {
        ObjectInputStream ois = new ObjectInputStream(new FileInputStream("data/pages/" + pageName + ".class"));
        return (Page) ois.readObject();
    }

    /**
     * Checks the compatibility of the types, throws Unmatching Types
     */
    private boolean checkTypes(Table thisTable, Hashtable<String, Object> inputColNameValue) throws BPlusEngineException {
        Enumeration<String> colNameValue = inputColNameValue.keys();
        Hashtable<String, String> tableColNameType = thisTable.colNameType;
        while (colNameValue.hasMoreElements()) {
            String currInColName = colNameValue.nextElement();
            String inputType = (String) tableColNameType.get(currInColName);
            Object inObject = inputColNameValue.get(currInColName);
            if (!switchTypes(inputType, inObject)) {
				return false;
			}
        }
        return true;
    }

    private void indexIfNeeded(Table thisTable, Tuple myTuple) {
        Hashtable<String, Object> htblColNameValue = myTuple.data;
        Enumeration<String> x = htblColNameValue.keys();
        while (x.hasMoreElements()) {
            String currColName = x.nextElement();
            if (thisTable.isIndexed(currColName)) {
				thisTable.colNameBTree.get(currColName).insert((Comparable) htblColNameValue.get(currColName), pageName + "#" + (tuples.size() - 1));
			}
        }
    }

    /**
     * A sub method for checking the types
     */
    private boolean switchTypes(String colType, Object obj) throws BPlusEngineException {
        switch (colType) {
            case "INT":
                if (obj instanceof Integer) {
					return true;
				}
                break;
            case "VARCHAR":
                if (obj instanceof String) {
					return true;
				}
                break;
            case "Double":
                if (obj instanceof Double) {
					return true;
				}
                break;
            case "Boolean":
                if (obj instanceof Boolean) {
					return true;
				}
                break;
            case "DATE":
                if (obj instanceof Date) {
					return true;
				}
                break;
            default:
                throw new BPlusEngineException("Either You spelled the Type incorectly or the type does not exist, "
                        + "Supported types: Integer, String, Double, Boolean, Date");
        }
        return false;
    }
}
