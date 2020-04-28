package com.my.database.storage.lsmdb.io.sstable.blocks;

import com.my.database.storage.lsmdb.utils.Modification;

import java.util.NoSuchElementException;

public abstract class AbstractSSTableBlock {

    public abstract Modification get(String row) throws NoSuchElementException;
}
