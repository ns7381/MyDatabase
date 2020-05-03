package com.my.database.bplus.tree;

import com.my.database.bplus.operator.Tuple;
import org.junit.Test;

import java.util.Hashtable;

public class BPlusTreeTest {

    @Test
    public void test() {

        //worked on m3 btree only because it used tuple as value
        BPlusTree x = new BPlusTree(4);

        Hashtable<String, Object> col = new Hashtable<String, Object>();
        col.put("ID", Integer.valueOf("1"));
        col.put("Name", "Data Bases II");
        col.put("Code", "CSEN 604");
        col.put("Hours", Integer.valueOf("4"));
        col.put("Semester", Integer.valueOf("6"));
        col.put("Major_ID", Integer.valueOf("2"));

		x.insert(3, String.valueOf(new Tuple(col)));
//		x.insert(1, new tuple(col));
//		x.insert(2, new tuple(col));
//		x.insert(4, new tuple(col));
//		x.insert(24, new tuple(col));
//		x.insert(4123, new tuple(col));
//		x.insert(2124, new tuple(col));
//		x.insert(4123, new tuple(col));
//		x.insert(234, new tuple(col));
        System.out.println(x.search(3) + "found");
        System.out.println(x.search(2) + "found");
        System.out.println(x.search(1) + "found");
        System.out.println(x.search(4) + "found");
        System.out.println(x.search(2124) + "found");
        System.out.println(x.search(16));
        System.out.println(x.search(18));
        System.out.println(x.search(20));
        System.out.println(x.search(234) + "found");
    }
}
