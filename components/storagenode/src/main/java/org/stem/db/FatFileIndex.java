/*
 * Copyright 2014 Alexey Plotnik
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.stem.db;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.zip.CRC32;

/**
 * Index entry format:
 * <p/>
 * 0                                16       20       24
 * +--------------------------------+--------+--------+
 * | key                            | offset | length |
 * +--------------------------------+--------+--------+
 * <p/>
 * Index format for 2 entries:
 * <p/>
 * 0       24      48      56
 * +-------+-------+-------+
 * | Entry | Entry | Meta  |
 * +-------+-------+-------+
 * <p/>
 * Index meta format:
 * <p/>
 * 0              4              8
 * +--------------+--------------+
 * | Index length | CRC32        |
 * +--------------+--------------+
 */
public class FatFileIndex {

    public static final int LENGTH_SIZE = 4;
    public static final int CRC32_SIZE = 4;

    private CRC32 crc = new CRC32();

    private List<Entry> entries;

    public List<Entry> getEntries() {
        return entries;
    }

    public FatFileIndex() {
        entries = new ArrayList<Entry>();
    }

    public void add(Entry entry) {
        entries.add(entry);
    }

    public int getSize() {
        return entries.size() * Entry.SIZE;
    }

    public ByteBuffer serialize() {
        ByteBuffer bodyBuf = ByteBuffer.allocate(getSize());
        for (Entry entry : entries) {
            entry.writeToBuf(bodyBuf);
        }
        bodyBuf.position(0);

        crc.update(bodyBuf.array());
        int crc32 = (int) crc.getValue();
        Header header = Header.create(getSize(), crc32);

        ByteBuffer indexBuf = ByteBuffer.allocate(getSize() + Header.SIZE);
        indexBuf.put(bodyBuf);
        indexBuf.put(header.serialize());

        return indexBuf;
    }

    public static Entry create(Blob.Header header, int blobOffset, byte deleteFlag) {
        return new Entry(header.key, blobOffset, header.length, deleteFlag);
    }

    public static FatFileIndex deserialize(FileChannel channel, long indexHeaderOffset) throws IOException {
        FileLock lock = channel.lock();
        try {
            // Deserialize header
            ByteBuffer out = ByteBuffer.allocate(FatFileIndex.Header.SIZE);
            channel.position(indexHeaderOffset);
            channel.read(out);
            out.position(0);
            Header header = Header.deserialize(out);

            FatFileIndex index = new FatFileIndex();
            out = ByteBuffer.allocate(header.length);

            long indexBodyOffset = indexHeaderOffset - header.length;
            channel.position(indexBodyOffset);
            channel.read(out);
            channel.position(0);

            CRC32 crc = new CRC32();
            crc.update(out.array());
            int crc32 = (int) crc.getValue();

            if (crc32 != header.crc32) {
                throw new IOException(String.format("Wrong index CRC32 checksum (%s but %s expected)", crc32, header.crc32));
            }

            out.position(0);
            while (out.hasRemaining()) {
                index.add(Entry.readFromBuf(out));
            }

            return index;
        } finally {
            lock.release();
        }
    }

    public Entry findByKey(byte[] key) {
        for (Entry entry : entries) {
            if (Arrays.equals(entry.key, key))
                return entry;
        }
        return null;
    }

    public static class Header {

        public static final int CRC32_SIZE = 4;
        public static final int LENGTH_SIZE = 4;
        public static final int SIZE = LENGTH_SIZE + CRC32_SIZE;

        int length;
        int crc32;

        public Header(int length, int crc32) {
            this.length = length;
            this.crc32 = crc32;
        }

        public ByteBuffer serialize() {
            ByteBuffer buf = ByteBuffer.allocate(SIZE);
            buf.putInt(length);
            ByteBuffer crc32Buf = ByteBuffer.allocate(CRC32_SIZE).putInt(crc32);
            crc32Buf.position(0);
            buf.put(crc32Buf);
            buf.position(0);

            return buf;
        }

        public static Header deserialize(ByteBuffer buf) {
            assert buf.remaining() == SIZE;
            int size = buf.getInt();
            int crc32 = buf.getInt();
            return Header.create(size, crc32);
        }

        public static Header create(int size, int crc32) {
            return new Header(size, crc32);
        }
    }

    public static class Entry {

        public static final int KEY_SIZE = 16;
        public static final int OFFSET_SIZE = 4;
        public static final int LENGTH_SIZE = 4;
        public static final byte FLAGS_SIZE = 1;

        public static final byte FLAG_LIVE = 0;
        public static final byte FLAG_DELETED = 1;


        public byte[] key; // 16
        public int offset; // 4
        public int length;  // 4
        public byte deleteFlag;  // 1
        public static final int SIZE = KEY_SIZE + OFFSET_SIZE + LENGTH_SIZE + FLAGS_SIZE;

        public Entry(byte[] key, int offset, int length, byte deleteFlag) {
            this.key = key;
            this.offset = offset;
            this.length = length;
            this.deleteFlag = deleteFlag;
        }

        public static Entry readFromBuf(ByteBuffer buf) {
            byte[] key = new byte[KEY_SIZE];
            buf.get(key);
            int offset = buf.getInt();
            int length = buf.getInt();
            byte deleteFlag = buf.get();
            return new Entry(key, offset, length, deleteFlag);
            // TODO: validation
        }

        public void writeToBuf(ByteBuffer buf) {
            buf.put(key);
            buf.putInt(offset);
            buf.putInt(length);
            buf.put(deleteFlag);
        }

        public boolean isLive() {
            return FLAG_LIVE == this.deleteFlag;
        }

        public boolean isDeleted() {
            return FLAG_DELETED == this.deleteFlag;
        }
    }
}
