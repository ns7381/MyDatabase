package com.my.database.storage.lsmdb.io.interfaces;

public interface WritableFilter extends Filter {
    void add(String key);
}
