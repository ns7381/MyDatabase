package com.my.database.storage.lsmdb.io.interfaces;

public interface Hasher<T> {
    long hash(T t);
}
