package com.my.database.storage.lsmdb.io.interfaces;

import java.io.IOException;

public interface Extractor<T> {
    T extract() throws IOException;
}
