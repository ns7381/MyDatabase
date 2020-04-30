package com.my.database.lsm.table;

import com.my.database.lsm.filter.hash.MurMurHasher;
import com.my.database.lsm.filter.hash.StringHasher;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.compress.compressors.CompressorStreamProvider;

import java.util.function.Function;

@Getter
@NoArgsConstructor
public final class Config {

    public static int blockBytesLimit = 1024 * 1024 * 64;

    public static int rowCacheCapacity = 1024 * 1024 * 64;
    public static int memTableBytesLimit = 128;

    public static int onDiskLevelsLimit = 3;
    public static int fileBufferSize = 256 * 256;

    public static int perBlockBloomFilterBits = 1024;
    public static StringHasher hash = new MurMurHasher();

    public static String compressorType = "gz";
    public static final String STORAGE_DIR = "/";
    public static CompressorStreamProvider compressorProvider = new CompressorStreamFactory();

    public static Function<Integer, Integer> blocksNumLimitForLevel = level -> ((int) Math.pow(10, level));
}
