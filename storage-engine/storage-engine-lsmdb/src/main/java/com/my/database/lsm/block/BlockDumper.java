package com.my.database.lsm.block;

import com.my.database.lsm.table.Config;
import com.my.database.lsm.table.Modification;
import com.my.database.lsm.table.Pair;
import com.my.database.lsm.filter.Filter;
import com.my.database.lsm.table.Modifications;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class BlockDumper {
    private static final int FILTER_BITS = Config.perBlockBloomFilterBits;

    /**
     * Dumps the modifications into the current temporary block. The number of longs in
     * the filter should match the filterBits.
     */
    public static void dump(Modifications modifications, Filter filter, File file) throws IOException {
        BufferedFileDumper dumper = null;
        dumper = new BufferedFileDumper(file);
        long[] longs = filter.toLongs();
        if (longs.length * Long.SIZE != FILTER_BITS) {
            throw new IOException("filter length mismatch");
        }
        dumper.writeFilter(filter);
        for (String row : modifications.keySet()) {
            dumper.writeString(row);
            Modification mod = modifications.get(row);
            if (mod.isPut()) {
                dumper.writeString(mod.getVal());
            } else {
                dumper.writeString("");
            }
            dumper.writeLong(mod.getTimestamp());
        }
        dumper.close();
    }

    public static void dump(List<Pair<String, String>> indexes, File file) throws IOException {
        BufferedFileDumper dumper = null;
        dumper = new BufferedFileDumper(file);
        for (Pair<String, String> range : indexes) {
            dumper.writeString(range.left);
            dumper.writeString(range.right);
        }
        dumper.close();
    }
}
