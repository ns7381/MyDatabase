package com.my.database.lsm.filter;

import com.my.database.lsm.table.Config;
import com.my.database.lsm.util.BitSet;
import com.my.database.lsm.filter.hash.StringHasher;

import java.nio.ByteBuffer;

public class BloomFilter implements WritableFilter {
    private BitSet bitset;
    private StringHasher hasher;

    public BloomFilter() {
        this.bitset = new BitSet(Config.perBlockBloomFilterBits);
        this.hasher = Config.hash;
    }

    public BloomFilter(long[] words) {
        this.bitset = new BitSet(words);
        this.hasher = Config.hash;
    }

    public BloomFilter(int numBits, StringHasher hasher) {
        this.bitset = new BitSet(numBits);
        this.hasher = hasher;
    }

    @Override
    public boolean isPresent(String key) {
        for (int off : bitOffsets(key)) {
            if (!bitset.get(off)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void add(String key) {
        for (int off : bitOffsets(key)) {
            bitset.set(off);
        }
    }

    private int bitOffset(String key) {
        long hash = hasher.hash(key);
        return ((int) Long.remainderUnsigned(hash, bitset.numLongs() * Long.SIZE));
    }

    private int[] bitOffsets(String key) {
        int[] offsets = new int[1];
        long hash = hasher.hash(key);
        for (int i = 0; i < 1; i++) {
            offsets[i] = bitOffset(hash);
            ByteBuffer bf = ByteBuffer.allocate(Long.BYTES);
            bf.putLong(hash);
            hash = hasher.hash(new String(bf.array()));
        }
        return offsets;
    }

    private int bitOffset(long hash) {
        return ((int) Long.remainderUnsigned(hash, bitset.numLongs() * Long.SIZE));
    }

    @Override
    public long[] toLongs() {
        return bitset.toLongs();
    }
}
