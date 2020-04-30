package com.my.database.lsm.filter.hash;

public interface StringHasher extends Hasher<String> {
    @Override
    long hash(String key);
}
