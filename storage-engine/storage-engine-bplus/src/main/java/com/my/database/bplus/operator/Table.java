package com.my.database.bplus.operator;


import com.my.database.bplus.exception.BPlusEngineException;
import com.my.database.bplus.tree.BPlusTree;

import java.io.*;
import java.util.Enumeration;
import java.util.Hashtable;

public class Table implements Serializable {
    public String tableName;
    public int pageCount = 1;
    public String primaryKey;
    Hashtable<String, String> colNameType;
    Hashtable<String, String> colNameRefs;
    Hashtable<String, BPlusTree> colNameBTree = new Hashtable<>();


    public Table(String tableName, Hashtable<String, String> colType, Hashtable<String, String> colRef, String key) throws IOException {
        this.tableName = tableName;
        this.colNameType = colType;
        this.primaryKey = key;
        this.colNameRefs = colRef;
        colNameBTree.put(key, new BPlusTree(Database.BTreeDegree));
        new Page(Database.configSize, tableName + "#" + pageCount);
        saveCsv();
        save();
    }

    public Hashtable<String, BPlusTree> getBTreeIndexes() {
        return colNameBTree;
    }

    public boolean isIndexed(String strColName) {
        return colNameBTree.containsKey(strColName);
    }

    public void incrementPageCount() throws IOException {
        pageCount++;
        save();
    }

    /**
     * returns the 1st Level Dens Index of Primary Keys
     */
    public BPlusTree getPrimaryKeys() {
        return colNameBTree.get(primaryKey);
    }


    /**
     * saves the current table to disk as "tableName_meta.class"
     */
    public void save() throws IOException {
        File f = new File("data/tableMeta/" + tableName + "_meta" + ".class");
        if (!f.exists()) {
            if (!f.getParentFile().exists()) {
                if (f.getParentFile().mkdirs()) {
                    f.createNewFile();
                }
            }
        }
        ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(f));
        oos.writeObject(this);
        oos.flush();
        oos.close();
    }

    /**
     * Saves the Table Headers to the CSV file as metaData
     */
    public void saveCsv() throws IOException {
        StringBuilder sb = new StringBuilder();
        Enumeration<String> colNames = colNameType.keys();
        while (colNames.hasMoreElements()) {
            String currColKey = colNames.nextElement();
            sb.append(tableName).append(",");
            sb.append(currColKey).append(",");
            sb.append(colNameType.get(currColKey)).append(",");
            if (this.primaryKey.equals(currColKey)) {
                sb.append("True");
            } else {
                sb.append("False");
            }
            sb.append(",");
            sb.append(isIndexed(currColKey));
            sb.append(",");
            // References
            if (colNameRefs.containsKey(currColKey)) {
                sb.append("True");
            } else {
                sb.append("False");
            }

            sb.append("\n");
        }
        FileWriter fileWriter = new FileWriter("data/metadata.csv", true);
        fileWriter.append(sb);
        fileWriter.flush();
        fileWriter.close();
    }

    /**
     * loads table from disk. "tableName"
     */
    public static Table load(String tableName) throws FileNotFoundException, IOException, ClassNotFoundException {
        ObjectInputStream ois = new ObjectInputStream(
                new FileInputStream("data/tableMeta/" + tableName + "_meta" + ".class"));
        return (Table) ois.readObject();
    }
}
