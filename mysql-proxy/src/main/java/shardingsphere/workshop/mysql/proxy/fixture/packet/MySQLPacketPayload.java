
package shardingsphere.workshop.mysql.proxy.fixture.packet;

import com.google.common.base.Strings;
import io.netty.buffer.ByteBuf;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * MySQL payload operation for MySQL packet data types.
 *
 * @see <a href="https://dev.mysql.com/doc/internals/en/describing-packets.html">describing packets</a>
 */
@RequiredArgsConstructor
@Getter
public final class MySQLPacketPayload implements AutoCloseable {
    
    private final ByteBuf byteBuf;
    
    /**
     * Read 1 byte fixed length integer from byte buffers.
     * 
     * @see <a href="https://dev.mysql.com/doc/internals/en/integer.html#packet-Protocol::FixedLengthInteger">FixedLengthInteger</a>
     * 
     * @return 1 byte fixed length integer
     */
    public int readInt1() {
        return byteBuf.readByte() & 0xff;
    }
    
    /**
     * Write 1 byte fixed length integer to byte buffers.
     * 
     * @see <a href="https://dev.mysql.com/doc/internals/en/integer.html#packet-Protocol::FixedLengthInteger">FixedLengthInteger</a>
     * 
     * @param value 1 byte fixed length integer
     */
    public void writeInt1(final int value) {
        byteBuf.writeByte(value);
    }
    
    /**
     * Read 2 byte fixed length integer from byte buffers.
     * 
     * @see <a href="https://dev.mysql.com/doc/internals/en/integer.html#packet-Protocol::FixedLengthInteger">FixedLengthInteger</a>
     *
     * @return 2 byte fixed length integer
     */
    public int readInt2() {
        return byteBuf.readShortLE() & 0xffff;
    }
    
    /**
     * Write 2 byte fixed length integer to byte buffers.
     * 
     * @see <a href="https://dev.mysql.com/doc/internals/en/integer.html#packet-Protocol::FixedLengthInteger">FixedLengthInteger</a>
     *
     * @param value 2 byte fixed length integer
     */
    public void writeInt2(final int value) {
        byteBuf.writeShortLE(value);
    }
    
    /**
     * Read 3 byte fixed length integer from byte buffers.
     * 
     * @see <a href="https://dev.mysql.com/doc/internals/en/integer.html#packet-Protocol::FixedLengthInteger">FixedLengthInteger</a>
     *
     * @return 3 byte fixed length integer
     */
    public int readInt3() {
        return byteBuf.readMediumLE() & 0xffffff;
    }
    
    /**
     * Write 3 byte fixed length integer to byte buffers.
     * 
     * @see <a href="https://dev.mysql.com/doc/internals/en/integer.html#packet-Protocol::FixedLengthInteger">FixedLengthInteger</a>
     *
     * @param value 3 byte fixed length integer
     */
    public void writeInt3(final int value) {
        byteBuf.writeMediumLE(value);
    }
    
    /**
     * Read 4 byte fixed length integer from byte buffers.
     * 
     * @see <a href="https://dev.mysql.com/doc/internals/en/integer.html#packet-Protocol::FixedLengthInteger">FixedLengthInteger</a>
     *
     * @return 4 byte fixed length integer
     */
    public int readInt4() {
        return byteBuf.readIntLE();
    }
    
    /**
     * Write 4 byte fixed length integer to byte buffers.
     * 
     * @see <a href="https://dev.mysql.com/doc/internals/en/integer.html#packet-Protocol::FixedLengthInteger">FixedLengthInteger</a>
     *
     * @param value 4 byte fixed length integer
     */
    public void writeInt4(final int value) {
        byteBuf.writeIntLE(value);
    
    }
    
    /**
     * Read 6 byte fixed length integer from byte buffers.
     * 
     * @see <a href="https://dev.mysql.com/doc/internals/en/integer.html#packet-Protocol::FixedLengthInteger">FixedLengthInteger</a>
     *
     * @return 6 byte fixed length integer
     */
    public long readInt6() {
        long result = 0;
        for (int i = 0; i < 6; i++) {
            result |= ((long) (0xff & byteBuf.readByte())) << (8 * i);
        }
        return result;
    }
    
    /**
     * Write 6 byte fixed length integer to byte buffers.
     * 
     * @see <a href="https://dev.mysql.com/doc/internals/en/integer.html#packet-Protocol::FixedLengthInteger">FixedLengthInteger</a>
     *
     * @param value 6 byte fixed length integer
     */
    public void writeInt6(final int value) {
        // TODO
    }
    
    /**
     * Read 8 byte fixed length integer from byte buffers.
     * 
     * @see <a href="https://dev.mysql.com/doc/internals/en/integer.html#packet-Protocol::FixedLengthInteger">FixedLengthInteger</a>
     *
     * @return 8 byte fixed length integer
     */
    public long readInt8() {
        return byteBuf.readLongLE();
    }
    
    /**
     * Write 8 byte fixed length integer to byte buffers.
     * 
     * @see <a href="https://dev.mysql.com/doc/internals/en/integer.html#packet-Protocol::FixedLengthInteger">FixedLengthInteger</a>
     *
     * @param value 8 byte fixed length integer
     */
    public void writeInt8(final long value) {
        byteBuf.writeLongLE(value);
        
    }
    
    /**
     * Read lenenc integer from byte buffers.
     * 
     * @see <a href="https://dev.mysql.com/doc/internals/en/integer.html#packet-Protocol::LengthEncodedInteger">LengthEncodedInteger</a>
     *
     * @return lenenc integer
     */
    public long readIntLenenc() {
        int firstByte = readInt1();
        if (firstByte < 0xfb) {
            return firstByte;
        }
        if (0xfb == firstByte) {
            return 0;
        }
        if (0xfc == firstByte) {
            return byteBuf.readShortLE();
        }
        if (0xfd == firstByte) {
            return byteBuf.readMediumLE();
        }
        return byteBuf.readLongLE();
    }
    
    /**
     * Write lenenc integer to byte buffers.
     * 
     * @see <a href="https://dev.mysql.com/doc/internals/en/integer.html#packet-Protocol::LengthEncodedInteger">LengthEncodedInteger</a>
     *
     * @param value lenenc integer
     */
    public void writeIntLenenc(final long value) {
        if (value < 0xfb) {
            byteBuf.writeByte((int) value);
            return;
        }
        if (value < Math.pow(2, 16)) {
            byteBuf.writeByte(0xfc);
            byteBuf.writeShortLE((int) value);
            return;
        }
        if (value < Math.pow(2, 24)) {
            byteBuf.writeByte(0xfd);
            byteBuf.writeMediumLE((int) value);
            return;
        }
        byteBuf.writeByte(0xfe);
        byteBuf.writeLongLE(value);
    }
    
    /**
     * Read lenenc string from byte buffers.
     * 
     * @see <a href="https://dev.mysql.com/doc/internals/en/string.html#packet-Protocol::FixedLengthString">FixedLengthString</a>
     *
     * @return lenenc string
     */
    public String readStringLenenc() {
        int length = (int) readIntLenenc();
        byte[] result = new byte[length];
        byteBuf.readBytes(result);
        return new String(result);
    }
    
    /**
     * Read lenenc string from byte buffers for bytes.
     *
     * @see <a href="https://dev.mysql.com/doc/internals/en/string.html#packet-Protocol::FixedLengthString">FixedLengthString</a>
     *
     * @return lenenc bytes
     */
    public byte[] readStringLenencByBytes() {
        int length = (int) readIntLenenc();
        byte[] result = new byte[length];
        byteBuf.readBytes(result);
        return result;
    }
    
    /**
     * Write lenenc string to byte buffers.
     * 
     * @see <a href="https://dev.mysql.com/doc/internals/en/string.html#packet-Protocol::FixedLengthString">FixedLengthString</a>
     *
     * @param value fixed length string
     */
    public void writeStringLenenc(final String value) {
        if (Strings.isNullOrEmpty(value)) {
            byteBuf.writeByte(0);
            return;
        }
        writeIntLenenc(value.getBytes().length);
        byteBuf.writeBytes(value.getBytes());
    }
    
    /**
     * Write lenenc bytes to byte buffers.
     *
     * @param value fixed length bytes
     */
    public void writeBytesLenenc(final byte[] value) {
        if (0 == value.length) {
            byteBuf.writeByte(0);
            return;
        }
        writeIntLenenc(value.length);
        byteBuf.writeBytes(value);
    }
    
    /**
     * Read fixed length string from byte buffers.
     * 
     * @see <a href="https://dev.mysql.com/doc/internals/en/string.html#packet-Protocol::FixedLengthString">FixedLengthString</a>
     *
     * @param length length of fixed string
     * 
     * @return fixed length string
     */
    public String readStringFix(final int length) {
        byte[] result = new byte[length];
        byteBuf.readBytes(result);
        return new String(result);
    }
    
    /**
     * Read fixed length string from byte buffers and return bytes.
     *
     * @see <a href="https://dev.mysql.com/doc/internals/en/string.html#packet-Protocol::FixedLengthString">FixedLengthString</a>
     *
     * @param length length of fixed string
     *
     * @return fixed length bytes
     */
    public byte[] readStringFixByBytes(final int length) {
        byte[] result = new byte[length];
        byteBuf.readBytes(result);
        return result;
    }
    
    /**
     * Write variable length string to byte buffers.
     * 
     * @see <a href="https://dev.mysql.com/doc/internals/en/string.html#packet-Protocol::FixedLengthString">FixedLengthString</a>
     *
     * @param value fixed length string
     */
    public void writeStringFix(final String value) {
        byteBuf.writeBytes(value.getBytes());
    }
    
    /**
     * Write variable length bytes to byte buffers.
     * 
     * @see <a href="https://dev.mysql.com/doc/internals/en/secure-password-authentication.html#packet-Authentication::Native41">Native41</a>
     *
     * @param value fixed length bytes
     */
    public void writeBytes(final byte[] value) {
        byteBuf.writeBytes(value);
    }
    
    /**
     * Read variable length string from byte buffers.
     * 
     * @see <a href="https://dev.mysql.com/doc/internals/en/string.html#packet-Protocol::VariableLengthString">FixedLengthString</a>
     *
     * @return variable length string
     */
    public String readStringVar() {
        // TODO
        return "";
    }
    
    /**
     * Write fixed length string to byte buffers.
     * 
     * @see <a href="https://dev.mysql.com/doc/internals/en/string.html#packet-Protocol::VariableLengthString">FixedLengthString</a>
     *
     * @param value variable length string
     */
    public void writeStringVar(final String value) {
        // TODO
    }
    
    /**
     * Read null terminated string from byte buffers.
     * 
     * @see <a href="https://dev.mysql.com/doc/internals/en/string.html#packet-Protocol::NulTerminatedString">NulTerminatedString</a>
     *
     * @return null terminated string
     */
    public String readStringNul() {
        byte[] result = new byte[byteBuf.bytesBefore((byte) 0)];
        byteBuf.readBytes(result);
        byteBuf.skipBytes(1);
        return new String(result);
    }
    
    /**
     * Read null terminated string from byte buffers and return bytes.
     *
     * @see <a href="https://dev.mysql.com/doc/internals/en/string.html#packet-Protocol::NulTerminatedString">NulTerminatedString</a>
     *
     * @return null terminated bytes
     */
    public byte[] readStringNulByBytes() {
        byte[] result = new byte[byteBuf.bytesBefore((byte) 0)];
        byteBuf.readBytes(result);
        byteBuf.skipBytes(1);
        return result;
    }
    
    /**
     * Write null terminated string to byte buffers.
     * 
     * @see <a href="https://dev.mysql.com/doc/internals/en/string.html#packet-Protocol::NulTerminatedString">NulTerminatedString</a>
     *
     * @param value null terminated string
     */
    public void writeStringNul(final String value) {
        byteBuf.writeBytes(value.getBytes());
        byteBuf.writeByte(0);
    }
    
    /**
     * Read rest of packet string from byte buffers.
     * 
     * @see <a href="https://dev.mysql.com/doc/internals/en/string.html#packet-Protocol::RestOfPacketString">RestOfPacketString</a>
     *
     * @return rest of packet string
     */
    public String readStringEOF() {
        byte[] result = new byte[byteBuf.readableBytes()];
        byteBuf.readBytes(result);
        return new String(result);
    }
    
    /**
     * Write rest of packet string to byte buffers.
     * 
     * @see <a href="https://dev.mysql.com/doc/internals/en/string.html#packet-Protocol::RestOfPacketString">RestOfPacketString</a>
     *
     * @param value rest of packet string
     */
    public void writeStringEOF(final String value) {
        byteBuf.writeBytes(value.getBytes());
    }
    
    /**
     * Skip reserved from byte buffers.
     * 
     * @param length length of reserved
     */
    public void skipReserved(final int length) {
        byteBuf.skipBytes(length);
    }
    
    /**
     * Write null for reserved to byte buffers.
     * 
     * @param length length of reserved
     */
    public void writeReserved(final int length) {
        for (int i = 0; i < length; i++) {
            byteBuf.writeByte(0);
        }
    }
    
    @Override
    public void close() {
        byteBuf.release();
    }
}
