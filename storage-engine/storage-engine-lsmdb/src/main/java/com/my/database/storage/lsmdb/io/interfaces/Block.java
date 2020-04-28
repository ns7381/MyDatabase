package com.my.database.storage.lsmdb.io.interfaces;

import java.io.File;
import java.io.IOException;

public interface Block {
    File getFile() throws IOException;
}
