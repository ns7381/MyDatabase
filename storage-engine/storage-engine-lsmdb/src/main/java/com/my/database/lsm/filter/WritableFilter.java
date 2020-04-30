package com.my.database.lsm.filter;

public interface WritableFilter extends Filter {
    void add(String key);
}
