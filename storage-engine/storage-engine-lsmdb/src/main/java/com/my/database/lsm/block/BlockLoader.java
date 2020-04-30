package com.my.database.lsm.block;

import com.my.database.lsm.table.Config;
import com.my.database.lsm.table.Modification;
import com.my.database.lsm.table.Pair;
import com.my.database.lsm.filter.BloomFilter;
import com.my.database.lsm.filter.Filter;
import com.my.database.lsm.table.Modifications;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.NoSuchElementException;

public class BlockLoader {
    private static final int bloomFilterBits = Config.perBlockBloomFilterBits;

    public static List<Pair<String, String>> loadIndex(File indexFile) throws IOException {
        BufferedFileLoader loader = null;
        loader = new BufferedFileLoader(indexFile);
        ArrayList<Pair<String, String>> ranges = new ArrayList<>();
        while (!loader.available()) {
            String r1 = loader.readString();
            String r2 = loader.readString();
            if (r1 != null && r2 != null) {
                ranges.add(new Pair<>(r1, r2));
            }
        }
        Comparator<Pair<String, String>> comp = Pair.<String, String>comparator();
        for (int i = 0; i < ranges.size() - 1; i++) {
            assert comp.compare(ranges.get(i), ranges.get(i + 1)) <= 0;
        }
        loader.close();
        return ranges;
    }

    public static int lookupIndex(File indexFile, String row) {
        try {
            List<Pair<String, String>> ranges = loadIndex(indexFile);
            int lo = 0, hi = ranges.size();
            while (lo < hi) {
                int mid = lo + (hi - lo) / 2;
                Pair<String, String> range = ranges.get(mid);
                if (row.compareTo(range.right) > 0) {
                    lo = mid + 1;
                } else if (row.compareTo(range.left) < 0) {
                    hi = mid;
                } else {
                    return mid;
                }
            }
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
        return -1;
    }

    public static Modifications loadModifications(File blockFile) throws IOException {
        BufferedFileLoader loader = null;
        loader = new BufferedFileLoader(blockFile);
        for (int i = 0; i < bloomFilterBits / Long.SIZE; i++) {
            loader.readLong();
        }
        Modifications mods = new Modifications(Config.blockBytesLimit);
        while (!loader.available()) {
            String crow = loader.readString();
            String cval = loader.readString();
            long timestamp = loader.readLong();
            if (cval.length() == 0) {
                mods.put(crow, new Modification(timestamp));
            } else {
                mods.put(crow, new Modification(cval, timestamp));
            }
        }
        loader.close();
        return mods;
    }

    public static Modification loadModification(File blockFile, String row) throws IOException {
        BufferedFileLoader loader = null;
        loader = new BufferedFileLoader(blockFile);
        Filter readFilter = loader.readFilter(bloomFilterBits / Long.SIZE, BloomFilter::new);
        if (!readFilter.isPresent(row)) {
            throw new NoSuchElementException("no such element");
        }
        while (!loader.available()) {
            String crow = loader.readString();
            String cval = loader.readString();
            long timestamp = loader.readLong();
            if (crow.equals(row)) {
                if (cval.length() == 0) {
                    return new Modification(timestamp);
                } else {
                    return new Modification(cval, timestamp);
                }
            }
        }
        loader.close();
        throw new NoSuchElementException("no such element");
    }

}
