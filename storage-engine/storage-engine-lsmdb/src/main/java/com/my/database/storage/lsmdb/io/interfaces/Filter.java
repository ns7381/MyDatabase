package com.my.database.storage.lsmdb.io.interfaces;

public interface Filter {
    boolean isPresent(String key);

    long[] toLongs();
}
