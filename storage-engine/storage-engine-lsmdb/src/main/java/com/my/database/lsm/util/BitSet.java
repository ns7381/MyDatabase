package com.my.database.lsm.util;


public class BitSet {
    private final static int ADDRESS_BITS_PER_WORD = 6;
    private final static int BITS_PER_WORD = 1 << ADDRESS_BITS_PER_WORD;

    private long[] words;

    public BitSet(int numBits) {
        this.words = initWords(numBits);
    }

    public BitSet(long[] words) {
        this.words = words;
    }

    private static int wordOffset(int bitOffset) {
        return bitOffset >> ADDRESS_BITS_PER_WORD;
    }

    private static long[] initWords(int numBits) {
        return new long[wordOffset(numBits)];
    }

    public void set(int bitOffset) {
        checkBitOffset(bitOffset);
        int wordOffset = wordOffset(bitOffset);
        words[wordOffset] |= 1L << bitOffset;
    }

    public boolean get(int bitOffset) {
        checkBitOffset(bitOffset);
        int wordOffset = wordOffset(bitOffset);
        return (words[wordOffset] & (1L << bitOffset)) != 0;
    }

    private void checkBitOffset(int bitOffset) {
        if (bitOffset < 0) {
            throw new IndexOutOfBoundsException("bitOffset < 0: " + bitOffset);
        }
        if (bitOffset >= BITS_PER_WORD * words.length) {
            throw new IndexOutOfBoundsException("bitOffset > all bits: " + bitOffset);
        }
    }

    public final int numLongs() {
        return this.words.length;
    }

    public long[] toLongs() {
        return this.words;
    }
}
