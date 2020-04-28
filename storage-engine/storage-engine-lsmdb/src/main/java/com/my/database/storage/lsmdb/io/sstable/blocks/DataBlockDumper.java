package com.my.database.storage.lsmdb.io.sstable.blocks;

import com.my.database.storage.lsmdb.utils.Modification;
import com.my.database.storage.lsmdb.utils.Modifications;
import com.my.database.storage.lsmdb.io.interfaces.Filter;
import org.apache.commons.compress.compressors.CompressorException;

import java.io.IOException;

public class DataBlockDumper {
    private final TempDataBlock tmpDataBlock;
    private final int filterBits;
    private final boolean compressible;

    private DataBlockDumper(TempDataBlock tmpDataBlock, int filterBits, boolean compressible) {
        this.tmpDataBlock = tmpDataBlock;
        this.filterBits = filterBits;
        this.compressible = compressible;
    }

    public DataBlockDumper(TempDataBlock tmpDataBlock, int filterBits) {
        this(tmpDataBlock, filterBits, false);
    }



    /**
     * Dumps the modifications into the current temporary block. The number of longs in
     * the filter should match the filterBits.
     */
    public void dump(Modifications modifications, Filter filter) throws IOException {
        tmpDataBlock.requireFileExists();
        ComponentFile c = null;
        try {
            c = compressible ?
                    tmpDataBlock.getCompressibleWritableComponentFile() :
                    tmpDataBlock.getWritableComponentFile();
            long[] longs = filter.toLongs();
            if (longs.length * Long.SIZE != filterBits) {
                throw new IOException("filter length mismatch");
            }
            c.writeFilter(filter);
            for (String row : modifications.rows()) {
                c.writeString(row);
                Modification mod = modifications.get(row);
                if (mod.isPut()) {
                    assert mod.getIfPresent() != null;
                    c.writeString(mod.getIfPresent().get());
                } else {
                    c.writeString("");
                }
                c.writeLong(mod.getTimestamp());
            }
        } catch(CompressorException ce) {
            throw new IOException(ce.getMessage());
        } finally {
            ComponentFile.tryClose(c);
        }
    }
}
