package com.my.database.storage.lsmdb.io.sstable.blocks;

import com.my.database.storage.lsmdb.io.sstable.SSTableConfig;
import com.my.database.storage.lsmdb.utils.Modification;
import com.my.database.storage.lsmdb.utils.Modifications;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class DataBlockLoaderTest {
    private Descriptor desc = new Descriptor("base", "ns", "cf", new String[]{"col"});
    private SSTableConfig config = SSTableConfig.defaultConfig();

    @Test
    public void get() throws Exception {
        DataBlock block = new DataBlock(desc, "col", 0, 3, config);
        DataBlockLoader loader = new DataBlockLoader(
                block, config.getPerBlockBloomFilterBits(), config.getHasher());
        Modifications mod = loader.extractModifications(config.getBlockBytesLimit());
        for (String row : mod.rows()) {
            Modification fetch = loader.get(row);
            if (mod.get(row).isPut()) {
                System.out.println(fetch.getIfPresent().get() + " = " + mod.get(row).getIfPresent().get());
                System.out.println(fetch.getTimestamp() + " = " + mod.get(row).getTimestamp());
            } else {
                System.out.println("delete");
                System.out.println(fetch.getTimestamp() + " = " + mod.get(row).getTimestamp());
            }
            assertEquals(fetch, mod.get(row));
        }
    }

}
