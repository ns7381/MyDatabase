package com.my.database.storage.lsmdb.io.sstable.blocks;

import com.my.database.storage.lsmdb.io.sstable.SSTableConfig;

import java.io.File;
import java.io.IOException;
import java.util.Optional;

public class DataBlock extends AbstractBlock implements Comparable<DataBlock> {
    private final Descriptor desc;
    private String column;
    private int level, index;

    public DataBlock(Descriptor desc, String column, int level, int index, SSTableConfig config) {
        super(config);
        this.desc = desc;
        this.level = level;
        this.index = index;
        if (!desc.hasColumn(column)) {
            throw new RuntimeException("should not happen");
        }
        this.column = column;
    }

    static boolean isDataBlock(String filename, SSTableConfig config) {
        // <level>_<index>_Data.db
        String[] parts = filename.split("_");
        if (parts.length != 3) {
            return false;
        }
        return parts[2].endsWith("Data" + config.getBlockFilenameSuffix());
    }

    public static boolean isDataBlockForLevel(String filename, SSTableConfig config, int level) {
        // <level>_<index>_Data.db
        String[] parts = filename.split("_");
        if (parts.length != 3) {
            return false;
        }
        if (Integer.parseInt(parts[0]) != level) {
            return false;
        }
        return parts[2].endsWith("Data" + config.getBlockFilenameSuffix());
    }

    public static Optional<DataBlock> fromFileName(Descriptor desc, String column, String filename, SSTableConfig config) {
        String[] parts = filename.split("_");
        if (parts.length != 3) {
            return Optional.empty();
        }
        if (!parts[2].endsWith("Data" + config.getBlockFilenameSuffix())) {
            return Optional.empty();
        }
        try {
            int level = Integer.parseInt(parts[0]);
            int index = Integer.parseInt(parts[1]);
            return Optional.of(new DataBlock(desc, column, level, index, config));
        } catch (NumberFormatException nfe) {
            return Optional.empty();
        }
    }

    static String buildFilename(Descriptor desc, String column, int level, int index, SSTableConfig config) {
        return String.format(
                "%d_%d_Data%s", level, index, config.getBlockFilenameSuffix()
        );
    }

    public int getIndex() {
        return index;
    }

    @Override
    public File getFile() throws IOException {
        File dir = desc.getDir();
        File colDir = new File(dir, column);
        if (!colDir.exists()) {
            colDir.mkdirs();
        }
        String filename = buildFilename(desc, column, level, index, config);
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
