package com.my.database.lsm.filter.hash;

public interface Hasher<T> {
    long hash(T t);
}
