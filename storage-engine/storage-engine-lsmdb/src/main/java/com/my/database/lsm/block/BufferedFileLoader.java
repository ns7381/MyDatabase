package com.my.database.lsm.block;

import com.my.database.lsm.table.Config;
import com.my.database.lsm.filter.Filter;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.function.Function;

class BufferedFileLoader {
    private static final int BUFFER_SIZE = Config.fileBufferSize;
    private BufferedInputStream reader;

    BufferedFileLoader(File file) throws FileNotFoundException {
        reader = new BufferedInputStream(new FileInputStream(file), BUFFER_SIZE);
    }

    String readString() throws IOException {
        byte[] ibs = new byte[Integer.BYTES];
        // read the first 4 bytes to know the length of following bytes
        reader.read(ibs);
        ByteBuffer byteBuffer = ByteBuffer.allocate(Integer.BYTES);
        byteBuffer.put(ibs).flip();
        int len = byteBuffer.getInt();
        // read the following bytes
        byte[] bytes = new byte[len];
        reader.read(bytes);
        return new String(bytes);
    }

    long readLong() throws IOException {
        byte[] lbs = new byte[Long.BYTES];
        reader.read(lbs);
        ByteBuffer buf = ByteBuffer.allocate(Long.BYTES);
        buf.put(lbs).flip();
        return buf.getLong();
    }

    Filter readFilter(int numLongs, Function<long[], Filter> filterFactory) throws IOException {
        long[] filterLongs = new long[numLongs];
        byte[] array = new byte[Long.BYTES * numLongs];
        reader.read(array);

        ByteBuffer buf = ByteBuffer.allocate(Long.BYTES * numLongs);
        buf.put(array);
        buf.flip();
        for (int i = 0; i < numLongs; i++) {
            filterLongs[i] = buf.getLong();
        }
        return filterFactory.apply(filterLongs);
    }

    boolean available() throws IOException {
        return reader.available() <= 0;
    }

    void close() {
        try {
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
