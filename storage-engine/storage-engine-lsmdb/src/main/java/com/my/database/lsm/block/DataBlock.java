package com.my.database.lsm.block;

import com.my.database.lsm.table.Config;
import com.my.database.lsm.table.Descriptor;
import lombok.Getter;

import java.io.File;
import java.io.IOException;

@Getter
public class DataBlock implements Comparable<DataBlock> {
    private final Descriptor desc;
    private String column;
    private int level;
    private int index;
    private final static String BLOCK_FILENAME_SUFFIX = ".db";

    public DataBlock(Descriptor desc, String column, int level, int index) {
        this.desc = desc;
        this.level = level;
        this.index = index;
        this.column = column;
    }

    public static boolean isDataBlockForLevel(String filename, int level) {
        // <level>_<index>_Data.db
        String[] parts = filename.split("_");
        if (parts.length != 3) {
            return false;
        }
        if (Integer.parseInt(parts[0]) != level) {
            return false;
        }
        return parts[2].endsWith("Data" + BLOCK_FILENAME_SUFFIX);
    }

    public static DataBlock fromFileName(Descriptor desc, String column, String filename) {
        String[] parts = filename.split("_");
        if (parts.length != 3) {
            return null;
        }
        if (!parts[2].endsWith("Data" + BLOCK_FILENAME_SUFFIX)) {
            return null;
        }
        try {
            int level = Integer.parseInt(parts[0]);
            int index = Integer.parseInt(parts[1]);
            return new DataBlock(desc, column, level, index);
        } catch (NumberFormatException nfe) {
            return null;
        }
    }

    private static String buildFilename(Descriptor desc, String column, int level, int index) {
        return String.format(
                "%d_%d_Data%s", level, index, BLOCK_FILENAME_SUFFIX
        );
    }

    public File getFile() throws IOException {
        File dir = new File(new File(Config.STORAGE_DIR + desc.getTable(), desc.getNs()), desc.getCf());
        if (!dir.exists()) {
            dir.mkdirs();
        }
        File colDir = new File(dir, column);
        if (!colDir.exists()) {
            colDir.mkdirs();
        }
        String filename = buildFilename(desc, column, level, index);
        return new File(colDir, filename);
    }

    @Override
    public int compareTo(DataBlock that) {
        int colCmp = this.column.compareTo(that.column);
        if (colCmp < 0) {
            return -1;
        }
        if (colCmp > 0) {
            return 1;
        }
        if (this.level < that.level) {
            return -1;
        }
        if (this.level > that.level) {
            return 1;
        }
        return Integer.compare(this.index, that.index);
    }
}
