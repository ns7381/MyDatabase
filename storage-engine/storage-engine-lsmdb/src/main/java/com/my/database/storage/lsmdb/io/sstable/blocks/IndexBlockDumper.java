package com.my.database.storage.lsmdb.io.sstable.blocks;

import com.my.database.storage.lsmdb.utils.Pair;

import java.io.IOException;
import java.util.List;

public class IndexBlockDumper {
    private IndexBlock idx;

    public IndexBlockDumper(IndexBlock idx) {
        this.idx = idx;
    }

    public void dump(List<Pair<String, String>> ranges) throws IOException {
        idx.requireFileExists();
        ComponentFile c = null;
        try {
            c = idx.getWritableComponentFile();
            for (Pair<String, String> range : ranges) {
//                System.out.println("put: " + range);
                c.writeString(range.left);
                c.writeString(range.right);
            }
        } finally {
            ComponentFile.tryClose(c);
        }
    }
}
