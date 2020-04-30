package com.my.database.lsm.block;

import com.my.database.lsm.table.Config;
import com.my.database.lsm.table.Descriptor;
import lombok.Getter;

import java.io.File;
import java.io.IOException;

@Getter
public class TempDataBlock implements Comparable<TempDataBlock> {
    private final Descriptor desc;
    private final String column;
    private int level, index, originIndex;
    private final static String TEMP_BLOCK_FILENAME_SUFFIX = ".db.tmp";

    public TempDataBlock(Descriptor desc, String column, int level, int index, int originIndex) {
        this.desc = desc;
        this.level = level;
        this.index = index;
        this.originIndex = originIndex;
        this.column = column;
    }

    public static boolean isTempDataBlock(String filename) {
        String[] parts = filename.split("_");
        // <level>_<originIndex>_Data.db.tmp_<index>
        if (parts.length != 4) {
            return false;
        }
        if (!parts[2].equals("Data" + TEMP_BLOCK_FILENAME_SUFFIX)) {
            return false;
        }
        try {
            int level = Integer.parseInt(parts[0]);
            int originIndex = Integer.parseInt(parts[1]);
            int index = Integer.parseInt(parts[3]);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    public static TempDataBlock fromFileName(Descriptor desc, String column, String filename) {
        String[] parts = filename.split("_");
        if (parts.length != 4) {
            return null;
        }
        if (!parts[2].equals("Data" + TEMP_BLOCK_FILENAME_SUFFIX)) {
            return null;
        }
        try {
            // <level>_<originIndex>_Data.db.tmp_<index>
            int level = Integer.parseInt(parts[0]);
            int originIndex = Integer.parseInt(parts[1]);
            int index = Integer.parseInt(parts[3]);
            return new TempDataBlock(desc, column, level, index, originIndex);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    public File getFile() {
        File dir = new File(new File(Config.STORAGE_DIR + desc.getTable(), desc.getNs()), desc.getCf());
        if (!dir.exists()) {
            dir.mkdirs();
        }
        File colDir = new File(dir, column);
        // <level>_<originIndex>_Data.db.tmp_<index>
        String filename = String.format(
                "%d_%d_Data%s_%d", level, originIndex, TEMP_BLOCK_FILENAME_SUFFIX, index
        );
        return new File(colDir, filename);
    }

    @Override
    public int compareTo(TempDataBlock that) {
        if (that == null) {
            throw new IllegalArgumentException("the block compare to is null");
        }
        if (this.level < that.level) {
            return -1;
        }
        if (this.level > that.level) {
            return 1;
        }
        if (this.originIndex < that.originIndex) {
            return -1;
        }
        if (this.originIndex > that.originIndex) {
            return 1;
        }
        return Integer.compare(this.index, that.index);
    }
}
