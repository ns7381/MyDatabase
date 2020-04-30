package com.my.database.lsm.filter;

public interface Filter {
    boolean isPresent(String key);

    long[] toLongs();
}
