package com.my.database.bplus.tree;

public class LeafEntry extends Entry {
    private String value;
    private boolean isDeleted;

    public boolean isDeleted() {
        return isDeleted;
    }

    public void setDeleted(boolean isDeleted) {
        this.isDeleted = isDeleted;
    }

    public String getValue() {
        return value;
    }

    public LeafEntry(Comparable key, String value) {
        super(key);
        this.value = value;
    }
}
