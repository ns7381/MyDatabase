package com.my.database.lsm.block;

import com.my.database.lsm.table.Config;
import com.my.database.lsm.filter.Filter;

import java.io.*;
import java.nio.ByteBuffer;

public class BufferedFileDumper {
    private static final int BUFFER_SIZE = Config.fileBufferSize;
    private BufferedOutputStream writer;

    public BufferedFileDumper(File file) throws FileNotFoundException {
        writer = new BufferedOutputStream(new FileOutputStream(file), BUFFER_SIZE);
    }

    /**
     * Write the BloomFilter of the SSTableBlock as 128 bit longs
     */
    void writeFilter(Filter filter) throws IOException {
        long[] filterLongs = filter.toLongs();
        ByteBuffer buf = ByteBuffer.allocate(Long.BYTES * filterLongs.length);
        for (long filterLong : filterLongs) {
            buf.putLong(filterLong);
        }
        buf.flip();
        writer.write(buf.array());
    }

    void writeString(String s) throws IOException {
        byte[] bytes = s.getBytes();
        ByteBuffer buf = ByteBuffer.allocate(Integer.BYTES + bytes.length);
        buf.putInt(bytes.length);
        buf.put(bytes);
        buf.flip();
        writer.write(buf.array());
    }

    void writeLong(long l) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(Long.BYTES);
        buf.putLong(l);
        buf.flip();
        writer.write(buf.array());
    }

    void close() {
        try {
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
