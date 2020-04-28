package com.my.database.storage.lsmdb.io.sstable.blocks;

import com.my.database.storage.lsmdb.io.sstable.SSTableConfig;
import org.junit.Test;

import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TempDataBlockTest {
    private Descriptor desc = new Descriptor("base", "ns", "cf", new String[]{"col"});
    private SSTableConfig config = SSTableConfig.defaultConfig();

    @Test
    public void getIndex() throws Exception {
        TempDataBlock t = new TempDataBlock(desc, "col", 0, 0, 1, config);
        assertEquals(t.getIndex(), 0);
    }

    @Test
    public void getOriginIndex() throws Exception {
        TempDataBlock t = new TempDataBlock(desc, "col", 0, 0, 1, config);
        assertEquals(t.getOriginIndex(), 1);
    }

    @Test
    public void isTempDataBlock() throws Exception {

    }

    @Test
    public void testRequire() throws Exception {
        TempDataBlock t = new TempDataBlock(desc, "col", 0, 0, 1, config);
        t.requireFileExists();
        assertTrue(t.getFile().exists());
    }

    @Test
    public void fromFileName() throws Exception {
        TempDataBlock t = new TempDataBlock(desc, "col", 0, 1, 10, config);
        t.requireFileExists();
        Optional<TempDataBlock> opt = TempDataBlock.fromFileName(desc, "col", "0_10_Data.db.tmp_1", config);
        assertTrue(opt.isPresent());
        assertEquals(opt.get().compareTo(t), 0);
    }

}
