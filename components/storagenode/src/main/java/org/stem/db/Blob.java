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

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.stem.domain.BlobDescriptor;
import org.stem.domain.ExtendedBlobDescriptor;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.zip.CRC32;

public class Blob // TODO: integrate all descriptors (mountpoint, fatfile, etc)
{
    Header header;
    byte[] data;

    BlobDescriptor descriptor;

    public Header getHeader() {
        return header;
    }

    public BlobDescriptor getDescriptor() {
        return descriptor;
    }

    public static Blob deserialize(FatFile ff, long blobHeaderOffset) throws IOException {
        FileChannel channel = ff.getReader().getChannel();
        Header header = Header.deserialize(channel, blobHeaderOffset);
        //System.out.println("deserialized: 0x" + Hex.encodeHexString(header.key) + ", offset=" + (int) blobHeaderOffset + Header.SIZE + ", valid=" + header.valid());
//        boolean valid = header.valid();
//        if (!valid) {
//            valid = header.valid();
//            if (valid) {
//                int a = 1;
//            }
//        }
        if (header.corrupted()) {
            return null;
        }
        //throw new IOException("Blob header is corrupted");

        int bodyOffset = (int) blobHeaderOffset + Header.SIZE;
        byte[] data = ff.readBlob(bodyOffset, header.length); // TODO: long to int conversion. Why?

        return new Blob(header, new BlobDescriptor(ff.id, (int) blobHeaderOffset, bodyOffset), data);
    }

    public static ExtendedBlobDescriptor deserializeDescriptor(FatFile ff, long blobHeaderOffset) throws IOException {
        FileChannel channel = ff.getReader().getChannel();
        Header header = Header.deserialize(channel, blobHeaderOffset);
        if (!header.valid()) {
            return null;
        }

        return new ExtendedBlobDescriptor(header.key, header.length, ff.id, (int) blobHeaderOffset, (int) blobHeaderOffset + Header.SIZE);
    }

    public Blob(Header header, byte[] data) {
        this.header = header;
        this.data = data;
    }

    public Blob(Header header, BlobDescriptor descriptor, byte[] data) {
        this(header, data);
        this.descriptor = descriptor;
    }

    public int size() {
        return header.length;
    }

    public byte[] key() {
        return header.key;
    }

    public byte[] data() {
        return data;
    }

    public boolean deleted() {
        return header.isDeleted();
    }

    public static class Header {
        private static final int KEY_SIZE = 16;
        private static final int LENGTH_SIZE = 4;
        private static final int CRC32_SIZE = 4;
        private static final int DELETE_FLAG_SIZE = 1;
        public static final int SIZE = KEY_SIZE + LENGTH_SIZE + CRC32_SIZE + DELETE_FLAG_SIZE;

        public byte[] key;
        public Integer length;
        public Integer crc32;
        public byte deleteFlag;

        private CRC32 crc = new CRC32();


        public static Header create(byte[] keyBytes, int payloadLength, int crc32, byte deleteFlag) {
            return new Header(keyBytes, payloadLength, crc32, deleteFlag);
        }

        public static Header create(byte[] keyBytes, int payloadLength, byte deleteFlag) {
            return new Header(keyBytes, payloadLength, deleteFlag);
        }

        public static Header create(String key, int payloadLength, byte deleteFlag) throws IOException {
            byte[] keyBytes;
            try {
                keyBytes = Hex.decodeHex(key.toCharArray());
            }
            catch (DecoderException e) {
                throw new IOException("Can not decode the key " + key, e);
            }
            assert keyBytes.length == 16;

            return new Header(keyBytes, payloadLength, deleteFlag);
        }

        public Header(byte[] key, int length, int crc32, byte deleteFlag) {
            this.key = key;
            this.length = length;
            this.crc32 = crc32;
            this.deleteFlag = deleteFlag;
        }


        public Header(byte[] key, int length, byte deleteFlag) {
            this.key = key;
            this.length = length;
            this.deleteFlag = deleteFlag;
        }

        /**
         * 1. Write key (16 bytes)
         * 2. Write blob length (4 bytes)
         * 3. Write CRC32 (4 bytes) of key and blob length
         * 4. Write delete flag (1 byte)
         *
         * @return
         */
        public ByteBuffer serialize() {
            ByteBuffer buf = ByteBuffer.allocate(SIZE);
            buf.put(key);
            buf.putInt(length);

            if (null == crc32)
                crc32 = calculateChecksum();

            ByteBuffer crc32Buf = ByteBuffer.allocate(4).putInt(crc32);
            crc32Buf.position(0);
            buf.put(crc32Buf);

            buf.put(deleteFlag);

            return buf;
        }

        public static Header deserialize(FileChannel channel, long blobHeaderOffset) throws IOException {
            FileLock lock = channel.lock();
            try {
                channel.position(blobHeaderOffset);
                ByteBuffer buf = ByteBuffer.allocate(SIZE);
                channel.read(buf);
                buf.position(0);
                byte[] key = new byte[KEY_SIZE];
                buf.get(key);

                int length = buf.getInt();
                int crc32 = buf.getInt();
                byte deleteFlag = buf.get();
                return Header.create(key, length, crc32, deleteFlag);
            }
            finally {
                lock.release();
            }


        }

        private Integer calculateChecksum() {
            ByteBuffer blobLengthBuf = ByteBuffer.allocate(LENGTH_SIZE).putInt(length);
            blobLengthBuf.position(0);

            ByteBuffer keyAndLengthBuf = ByteBuffer.allocate(KEY_SIZE + LENGTH_SIZE).put(key).put(blobLengthBuf);

            CRC32 crc = new CRC32();
            crc.update(keyAndLengthBuf.array());
            return (int) crc.getValue();
        }

        public void nextOffset() {

        }

        public boolean valid() {
            return calculateChecksum().equals(crc32);
        }

        public boolean corrupted() {
            return !valid();
        }

        public FatFileIndex.Entry toIndexEntry(int offset) {
            return new FatFileIndex.Entry(key, offset, length, deleteFlag);
        }


        public boolean isLive() {
            return FatFileIndex.Entry.FLAG_LIVE == this.deleteFlag;
        }

        public boolean isDeleted() {
            return FatFileIndex.Entry.FLAG_DELETED == this.deleteFlag;
        }
    }
}
