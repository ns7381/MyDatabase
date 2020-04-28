package com.my.database.storage.lsmdb.io.sstable.blocks;

import com.my.database.storage.lsmdb.io.sstable.SSTableConfig;
import org.apache.commons.compress.compressors.CompressorException;

import java.io.File;
import java.io.IOException;
import java.util.Optional;

public class TempDataBlock extends AbstractBlock implements Comparable<TempDataBlock> {
    private final Descriptor desc;
    private final String column;
    private int level, index, originIndex;

    public TempDataBlock(
            Descriptor desc,
            String column,
            int level,
            int index,
            int originIndex,
            SSTableConfig config
    ) {
        super(config);
        this.desc = desc;
        this.level = level;
        this.index = index;
        this.originIndex = originIndex;
        this.column = column;
    }

    public static boolean isTempDataBlock(String filename, SSTableConfig config) {
        String[] parts = filename.split("_");
        // <level>_<originIndex>_Data.db.tmp_<index>
        if (parts.length != 4) {
            return false;
        }
        if (!parts[2].equals("Data" + config.getTempBlockFilenameSuffix())) {
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

    public static Optional<TempDataBlock> fromFileName(
            Descriptor desc,
            String column,
            String filename,
            SSTableConfig config
    ) {
        String[] parts = filename.split("_");
        // <level>_<originIndex>_Data.db.tmp_<index>
//        for (String p: parts) {
//            System.out.print(p + " ");
//        }
        if (parts.length != 4) {
            return Optional.empty();
        }
        if (!parts[2].equals("Data" + config.getTempBlockFilenameSuffix())) {
            return Optional.empty();
        }
        try {
            // <level>_<originIndex>_Data.db.tmp_<index>
            int level = Integer.parseInt(parts[0]);
            int originIndex = Integer.parseInt(parts[1]);
            int index = Integer.parseInt(parts[3]);
            return Optional.of(new TempDataBlock(desc, column, level, index, originIndex, config));
        } catch (NumberFormatException e) {
            return Optional.empty();
        }
    }

    int getIndex() {
        return index;
    }

    public int getOriginIndex() {
        return originIndex;
    }

    @Override
    public File getFile() throws IOException {
        File dir = desc.getDir();
        File colDir = new File(dir, column);
        // <level>_<originIndex>_Data.db.tmp_<index>
        String filename = String.format(
                "%d_%d_Data%s_%d", level, originIndex, config.getTempBlockFilenameSuffix(), index
        );
        return new File(colDir, filename);
    }

    ComponentFile getWritableComponentFile() throws IOException {
        requireFileWritable();
        return new ComponentFile(getFile(), "w", config.getFileBufferSize());
    }

    ComponentFile getCompressibleWritableComponentFile() throws IOException, CompressorException {
        requireFileWritable();
        return new ComponentFile(
                getFile(), "w",
                config.getFileBufferSize(),
                config.getCompressorProvider(),
                config.getCompressorType()
        );
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
