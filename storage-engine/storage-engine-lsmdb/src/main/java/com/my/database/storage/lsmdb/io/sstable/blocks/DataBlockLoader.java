package com.my.database.storage.lsmdb.io.sstable.blocks;

import com.my.database.storage.lsmdb.io.sstable.filters.BloomFilter;
import com.my.database.storage.lsmdb.utils.Modification;
import com.my.database.storage.lsmdb.utils.Modifications;
import com.my.database.storage.lsmdb.utils.Qualifier;
import com.my.database.storage.lsmdb.utils.Timed;
import com.my.database.storage.lsmdb.io.interfaces.Filter;
import com.my.database.storage.lsmdb.io.interfaces.StringHasher;
import org.apache.commons.compress.compressors.CompressorException;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

public class DataBlockLoader extends AbstractSSTableBlock {
    private final DataBlock dataBlock;
    private final int bloomFilterBits;
    private final StringHasher hasher;
    private final boolean compressible;

    public DataBlockLoader(DataBlock block, int bloomFilterBits, StringHasher hasher) {
        this(block, bloomFilterBits, hasher, false);
    }

    private DataBlockLoader(
            DataBlock block,
            int bloomFilterBits,
            StringHasher hasher,
            boolean compressible) {
        dataBlock = block;
        this.bloomFilterBits = bloomFilterBits;
        this.hasher = hasher;
        this.compressible = compressible;
    }

    @Override
    public Modification get(String row) throws NoSuchElementException {
        ComponentFile componentFile = null;
        try {
            componentFile = compressible ?
                    dataBlock.getCompressibleReadableComponentFile() :
                    dataBlock.getReadableComponentFile();
            Filter filter = componentFile.readFilter(bloomFilterBits / Long.SIZE, longs -> new BloomFilter(longs, hasher));
            if (!filter.isPresent(row)) {
                throw new NoSuchElementException("no such element");
            }
            while (!componentFile.eof()) {
                String crow = componentFile.readString();
                String cval = componentFile.readString();
                long timestamp = componentFile.readLong();
                if (crow.equals(row)) {
                    if (cval.length() == 0) {
                        return Modification.remove(timestamp);
                    } else {
                        return Modification.put(new Timed<>(cval, timestamp));
                    }
                }
            }
            throw new NoSuchElementException("no such element");
        } catch (IOException ioe) {
            throw new NoSuchElementException("could not find element due to IOException " + ioe.getMessage());
        } catch (CompressorException ce) {
            throw new NoSuchElementException(
                    "could not find element due to CompressionException " + ce.getMessage());
        } finally {
            ComponentFile.tryClose(componentFile);
        }
    }

    public Map<String, Timed<String>> getColumnWithQualifier(Qualifier q) throws IOException {
        Map<String, Timed<String>> column = new HashMap<>();
        Modifications mods = this.extractModifications(Integer.MAX_VALUE);
        for (Map.Entry<String, Modification> entry : mods.entrySet()) {
            String rowKey = entry.getKey();
            Modification mod = entry.getValue();
            if (!mod.isRemove()) {
                Timed<String> timedValue = mod.getIfPresent();
                if (q.qualify(rowKey, timedValue.get())) {
                    column.put(rowKey, timedValue);
                }
            }
        }
        return column;
    }

    public Modifications extractModifications(int limit) throws IOException {
        ComponentFile componentFile = null;
        try {
            componentFile =  compressible ?
                    dataBlock.getCompressibleReadableComponentFile() :
                    dataBlock.getReadableComponentFile();
            for (int i = 0; i < bloomFilterBits / Long.SIZE; i++) {
                componentFile.readLong();
            }
            Modifications mods = new Modifications(limit);
            while (!componentFile.eof()) {
                String crow = componentFile.readString();
                String cval = componentFile.readString();
                long timestamp = componentFile.readLong();
                if (cval.length() == 0) {
                    mods.put(crow, Modification.remove(timestamp));
                } else {
                    mods.put(crow, Modification.put(new Timed<>(cval, timestamp)));
                }
            }

            return mods;
        } catch (CompressorException ce) {
            throw new IOException(ce.getMessage());
        } finally {
            ComponentFile.tryClose(componentFile);
        }
    }
}
