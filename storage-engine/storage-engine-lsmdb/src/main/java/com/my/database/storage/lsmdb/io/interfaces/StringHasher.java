package com.my.database.storage.lsmdb.io.interfaces;

public interface StringHasher extends Hasher<String> {
    @Override
    long hash(String key);
}
