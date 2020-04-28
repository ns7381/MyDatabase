package com.my.database.storage.lsmdb.io.sstable;

import com.my.database.storage.lsmdb.io.interfaces.StringHasher;
import com.my.database.storage.lsmdb.utils.Modifications;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.compress.compressors.CompressorStreamProvider;

import java.util.LinkedList;
import java.util.function.Function;

public final class SSTableConfig {
    /**
     * 16 MB
     */
    private int blockBytesLimit = 1024 * 1024 * 16;

    /**
     * 16 MB
     */
    private int memTableBytesLimit = 1024 * 1024 * 16;

    private int perBlockBloomFilterBits = 1024;

    private int onDiskLevelsLimit = 3;

    private Function<LinkedList<MemTable>, Modifications> memTablesFlushStrategy = l -> {
        Modifications mod = new Modifications(blockBytesLimit);
        while (l.size() > 1) {
            mod.offer(l.removeFirst().stealModifications());
        }
        return mod;
    };

    private int memTablesLimit = 4;

    private Function<Integer, Integer> blocksNumLimitForLevel = l -> ((int) Math.pow(10, l));

    private StringHasher hasher = new MurMurHasher();

    private String blockFilenameSuffix = ".db";

    private String tempBlockFilenameSuffix = ".db.tmp";

    private int fileBufferSize = 256 * 256;

    public CompressorStreamProvider getCompressorProvider() {
        return compressorProvider;
    }

    public String getCompressorType() {
        return compressorType;
    }

    private CompressorStreamProvider compressorProvider = new CompressorStreamFactory();

    private String compressorType;

    private SSTableConfig() {
    }

    static SSTableConfigBuilder builder() {
        return new SSTableConfigBuilder();
    }

    public static SSTableConfig defaultConfig() {
        return new SSTableConfig();
    }

    public int getBlockBytesLimit() {
        return blockBytesLimit;
    }

    int getMemTableBytesLimit() {
        return memTableBytesLimit;
    }

    public int getPerBlockBloomFilterBits() {
        return perBlockBloomFilterBits;
    }

    int getOnDiskLevelsLimit() {
        return onDiskLevelsLimit;
    }

    int getMemTablesLimit() {
        return memTablesLimit;
    }

    public int getRowCacheCapacity() {
        return 1024;
    }

    Function<LinkedList<MemTable>, Modifications> getMemTablesFlushStrategy() {
        return memTablesFlushStrategy;
    }

    public Function<Integer, Integer> getBlocksNumLimitForLevel() {
        return blocksNumLimitForLevel;
    }

    public StringHasher getHasher() {
        return hasher;
    }

    public String getBlockFilenameSuffix() {
        return blockFilenameSuffix;
    }

    public String getTempBlockFilenameSuffix() {
        return tempBlockFilenameSuffix;
    }

    public int getFileBufferSize() {
        return fileBufferSize;
    }

    public static class SSTableConfigBuilder {
        private SSTableConfig config;

        private SSTableConfigBuilder() {
            config = new SSTableConfig();
        }

        public SSTableConfigBuilder setBlockBytesLimit(int blockBytesLimit) {
            config.blockBytesLimit = blockBytesLimit;
            return this;
        }

        public SSTableConfigBuilder setMemTableBytesLimit(int memTableBytesLimit) {
            config.memTableBytesLimit = memTableBytesLimit;
            return this;
        }

        public SSTableConfigBuilder setPerBlockBloomFilterBits(int perBlockBloomFilterBits) {
            config.perBlockBloomFilterBits = perBlockBloomFilterBits;
            return this;
        }

        public SSTableConfigBuilder setOnDiskLevelsLimit(int onDiskLevelsLimit) {
            config.onDiskLevelsLimit = onDiskLevelsLimit;
            return this;
        }

        public SSTableConfigBuilder setBlocksNumLimitForLevel(
                Function<Integer, Integer> blocksNumLimitForLevel) {
            config.blocksNumLimitForLevel = blocksNumLimitForLevel;
            return this;
        }

        public SSTableConfigBuilder setHasher(StringHasher hasher) {
            config.hasher = hasher;
            return this;
        }

        public SSTableConfigBuilder setBlockFilenameSuffix(String blockFilenameSuffix) {
            config.blockFilenameSuffix = blockFilenameSuffix;
            return this;
        }

        public SSTableConfigBuilder setTempBlockFilenameSuffix(String tempBlockFilenameSuffix) {
            config.tempBlockFilenameSuffix = tempBlockFilenameSuffix;
            return this;
        }

        SSTableConfigBuilder setMemTablesLimit(int limit) {
            config.memTablesLimit = limit;
            return this;
        }

        public SSTableConfigBuilder setMemTablesFlushStrategy(
                Function<LinkedList<MemTable>, Modifications> f) {
            config.memTablesFlushStrategy = f;
            return this;
        }

        public SSTableConfigBuilder setFileBufferSize(int size) {
            config.fileBufferSize = size;
            return this;
        }

        public SSTableConfigBuilder setCompressorType(String s) {
            config.compressorType = s;
            return this;
        }

        SSTableConfig build() {
            return config;
        }

    }
}
