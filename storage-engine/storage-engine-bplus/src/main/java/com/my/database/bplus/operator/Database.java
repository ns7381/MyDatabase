package com.my.database.bplus.operator;


import com.my.database.bplus.exception.BPlusEngineException;
import com.my.database.bplus.tree.BPlusTree;

import java.io.*;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;

public class Database {
    public static int configSize = 2000;
    public static int BTreeDegree = 4;
    public static Hashtable<String, Table> tableMeta = new Hashtable<>();

    public void init() throws Exception {
        File f = new File("data/metadata.csv");
        if (f.exists()) {
            loadMeta(f);
        } else {
            createMetaCsv();
        }
    }

    public void createTable(String tableName, Hashtable<String, String> colType,
                            Hashtable<String, String> colRef, String strKeyColName) throws BPlusEngineException, IOException {
        if (tableName.contains("#")) {
            throw new BPlusEngineException("You can not use the character # in a table's Name");
        }
        if (tableMeta.containsKey(tableName)) {
            return;
        }
        Table newTable = new Table(tableName, colType, colRef, strKeyColName);
        tableMeta.put(tableName, newTable);
    }

    public void createIndex(String strTableName, String strColName)
            throws ClassNotFoundException, BPlusEngineException, IOException {
        BPlusTree index = new BPlusTree(BTreeDegree);
        Table toIndexTable = tableMeta.get(strTableName);
        Iterator<Tuple> all = selectFromTable(strTableName, new Hashtable<String, Object>(), "or");
        int i = 0;
        while (all.hasNext()) {
            Tuple currTuple = all.next();
            index.insert((Comparable) currTuple.data.get(strColName),
                    strTableName + "#" + currTuple.getPageNo() + "#" + i % configSize);
            i++;
        }
        toIndexTable.colNameBTree.put(strColName, index);
        toIndexTable.save();
    }

    /**
     * Inserts into the table strTableName and saves it to disk immediately, your data is always safe
     */
    public void insertIntoTable(String strTableName, Hashtable<String, Object> col)
            throws BPlusEngineException, ClassNotFoundException, IOException {
        if (!tableMeta.containsKey(strTableName)) {
            throw new BPlusEngineException("Table Does not exist");
        }

        Table toInsert = tableMeta.get(strTableName);
        int pageNo = toInsert.pageCount;
        Page lastPage;

        lastPage = Page.load(strTableName + "#" + pageNo);
        if (!lastPage.isFull()) {
            lastPage.addToPage(col);
        } else {
            lastPage = new Page(configSize, strTableName + "#" + (pageNo + 1));
            lastPage.addToPage(col);
            toInsert.pageCount++; // updateTable;
            toInsert.save();
        }
        tableMeta.remove(strTableName);
        tableMeta.put(strTableName, toInsert);
    }

    /**
     * gets the page No from the primary key index and searches the tuples in
     * O(n) time to update the tuple and it's time
     */
    public void updateTable(String strTableName, Object keyToUpdateData, Hashtable<String, Object> htblColNameValue)
            throws ClassNotFoundException, IOException, BPlusEngineException {
        // Using Btree Done
        Table myTable = tableMeta.get(strTableName);
        BPlusTree thisTree = myTable.getPrimaryKeys();

        // wet
        Hashtable<String, Object> htbl = new Hashtable<String, Object>();
        htbl.put(myTable.primaryKey, keyToUpdateData);
        Iterator<Tuple> found = selectFromTable(strTableName, htbl, "OR");

        if (!found.hasNext()) {
            throw new BPlusEngineException("Item not found to update");
        }

        Tuple currTuple = found.next();
        if (!htblColNameValue.containsKey(myTable.primaryKey)) {
            Enumeration<String> keysToCompare = htblColNameValue.keys();
            while (keysToCompare.hasMoreElements()) {
                String currKey = keysToCompare.nextElement();
                Page x = Page.load(strTableName + "#" + currTuple.getPageNo());
                x.getTuples().get(currTuple.location).data.replace(currKey, htblColNameValue.get(currKey));
                x.save();
            }
        } else {
            System.out.println("Can not update primary key according to description");
        }
    }

    /**
     * to implement try keeping the code dry by using select
     */
    public void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue, String strOperator)
            throws BPlusEngineException, ClassNotFoundException, IOException {
        Hashtable indexes = tableMeta.get(strTableName).getBTreeIndexes();
        Enumeration<String> indexColumns = indexes.keys();
        Iterator<Tuple> toDelete = selectFromTable(strTableName, htblColNameValue, strOperator);
        boolean done = false;
        while (toDelete.hasNext()) {
            Tuple currTuple = toDelete.next();
            Page x = Page.load(strTableName + "#" + currTuple.getPageNo());
            currTuple.setDeleted(true);
            x.getTuples().get(currTuple.location).setDeleted(true);
            done = true;
            x.save();
            while (indexColumns.hasMoreElements()) {
                String colName = indexColumns.nextElement();
                if (currTuple.data.containsKey(colName)) {
                    ((BPlusTree) indexes.get(colName)).delete((Comparable) currTuple.data.get(colName));
                }
            }
            indexColumns = indexes.keys();
        }
        // delete from table
        if (!done) {
            selectOrDel(strTableName, htblColNameValue, strOperator, true);
        }
    }

    public Iterator<Tuple> selectFromTable(String strTable, Hashtable<String, Object> selectCol,
                                           String strOperator) throws ClassNotFoundException, IOException {
        strOperator = strOperator.toLowerCase();
        Table thisTable = tableMeta.get(strTable);
        if ("and".equals(strOperator)) {
            boolean byIndex = false;
            Enumeration<String> keySearch = selectCol.keys();
            while (keySearch.hasMoreElements()) {
                String currKeySearch = keySearch.nextElement();
                if (thisTable.colNameBTree.containsKey(currKeySearch)) {
                    byIndex = true;
                }

            }
            if (byIndex) {
                return selectByIndex(thisTable, selectCol, strOperator);
            }
        }
        return selectOrDel(strTable, selectCol, strOperator, false);
    }

    private Iterator<Tuple> selectByIndex(Table thisTable, Hashtable<String, Object> htblColNameValue,
                                          String strOperator) throws ClassNotFoundException, IOException {
        Enumeration<String> keys = htblColNameValue.keys();
        if (!keys.hasMoreElements()) {
            return displayTable(thisTable.tableName);
        }

        Hashtable<String, BPlusTree> indexes = thisTable.getBTreeIndexes();
        ArrayList<Tuple> list = new ArrayList<Tuple>();
        String currKey = keys.nextElement();
        if (!thisTable.colNameBTree.contains(currKey)) {
            while (keys.hasMoreElements()) {
                currKey = keys.nextElement();
                if (thisTable.colNameBTree.contains(currKey)) {
                    break;
                }
            }
        }
        ArrayList<String> foundPages = indexes.get(currKey).search((Comparable) htblColNameValue.get(currKey));
        ArrayList<Tuple> found = new ArrayList<Tuple>();

        for (int i = 0; i < foundPages.size(); i++) {
            String[] split = foundPages.get(i).split("#");
            Page currPage = Page.load(split[0] + "#" + split[1]);
            int location = Integer.parseInt(split[2]);
            Tuple currTuple = currPage.getTuples().get(location);
            if (!currTuple.isDeleted()) {
                found.add(currTuple);
            }
        }

        for (int i = 0; i < found.size(); i++) {
            boolean toSelect = true;
            while (keys.hasMoreElements()) {
                currKey = keys.nextElement();
                if (!found.get(i).data.get(currKey).equals(htblColNameValue.get(currKey))) {
                    toSelect = false;
                }
            }
            if (toSelect) {
                list.add(found.get(i));
            }
        }
        return list.iterator();
    }

    private Iterator<Tuple> displayTable(String tableName) throws ClassNotFoundException, IOException {
        ArrayList<Tuple> list = new ArrayList<Tuple>();
        Page myPage = Page.load(tableName + "#1");
        while (myPage != null) {
            ArrayList<Tuple> tuples = myPage.getTuples();
            for (int i = 0; i < tuples.size(); i++) {
                if (!tuples.get(i).isDeleted()) {
                    list.add(tuples.get(i));
                }
            }
            myPage = myPage.next();
        }
        return list.iterator();
    }

    private Iterator<Tuple> selectOrDel(String strTable, Hashtable<String, Object> selectCol, String strOperator,
                                        boolean delete) throws ClassNotFoundException, IOException {
        if (selectCol.isEmpty()) {
            return displayTable(strTable);
        }
        ArrayList<Tuple> list = new ArrayList<Tuple>();
        Page myPage = Page.load(strTable + "#1");
        while (myPage != null) {
            for (int j = 0; j < myPage.getTuples().size(); j++) {
                Tuple currTuple = myPage.getTuples().get(j);
                Enumeration<String> keysToCompare = selectCol.keys();
                switch (strOperator) {
                    case "and":
                        boolean toSelect = true;
                        while (keysToCompare.hasMoreElements()) {
                            String currKey = keysToCompare.nextElement();
                            if (!currTuple.data.get(currKey).equals(selectCol.get(currKey))) {
                                toSelect = false;
                                break;
                            }
                        }
                        if (toSelect) {
                            if (delete) {
                                myPage.removeTuple(j--);
                            } else {
                                if (!currTuple.isDeleted()) {
                                    list.add(currTuple);
                                }
                            }
                        }
                        break;
                    case "or":
                        while (keysToCompare.hasMoreElements()) {
                            String currKey = (String) keysToCompare.nextElement();
                            if (currTuple.data.get(currKey).equals(selectCol.get(currKey))) {
                                if (delete) {
                                    myPage.removeTuple(j--);
                                } else {
                                    if (!currTuple.isDeleted()) {
                                        list.add(currTuple);
                                    }
                                }
                                break;
                            }
                        }
                        break;
                    default:
                        break;
                }
            }
            myPage = myPage.next();
        }
        return (delete) ? null : list.iterator();
    }

    /**
     * Loads metadata from disk, which is the state of the app before terminating
     */
    private void loadMeta(File f) throws IOException, ClassNotFoundException {
        BufferedReader br = new BufferedReader(new FileReader(f));
        String line;
        line = br.readLine();
        String tableName = "";
        while ((line = br.readLine()) != null) {
            String[] parsedLine = line.split(",");
            if (!tableName.equals(parsedLine[0])) {
                tableName = parsedLine[0];
                ObjectInputStream ois = new ObjectInputStream(
                        new FileInputStream("data/tableMeta/" + tableName + "_meta.class"));
                tableMeta.put(tableName, (Table) ois.readObject());
            }
            tableName = parsedLine[0];
        }
    }

    /**
     * Creates the CSV file for storing table Headers
     */
    private void createMetaCsv() throws IOException {
        FileWriter fileWriter = new FileWriter("data/metadata.csv");
        // Write the CSV file header
        fileWriter.append("Table Name, Column Name, Column Type, Key, Indexed, References");
        // Add a new line separator after the header
        fileWriter.append("\n");
        fileWriter.flush();
        fileWriter.close();
    }
}
