package com.my.database.bplus.db;

import java.io.Serializable;
import java.util.Enumeration;
import java.util.Hashtable;

public class Tuple implements Serializable {
    Hashtable<String, Object> data;
    private boolean isDeleted;
    private int pageNo;
    int location;

    public Tuple(Hashtable<String, Object> htblColNameValue) {
        this.data = htblColNameValue;
    }

    public int getPageNo() {
        return pageNo;
    }

    public void setPageNo(int pageNo) {
        this.pageNo = pageNo;
    }

    public Hashtable<String, Object> getData() {
        return data;
    }

    public void setData(Hashtable<String, Object> data) {
        this.data = data;
    }

    public boolean isDeleted() {
        return isDeleted;
    }

    public void setDeleted(boolean isDeleted) {
        this.isDeleted = isDeleted;
    }

    @Override
    public String toString() {
        String sb = "";
        Enumeration<String> keys = data.keys();
        while (keys.hasMoreElements()) {
            String key = keys.nextElement();
            sb = sb + key + ": " + data.get(key) + ", ";
        }
        if (sb.length() > 2) {
			return sb.substring(0, sb.length() - 2);
		}
        return sb;
    }
}
