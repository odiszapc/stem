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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stem.domain.BlobDescriptor;
import org.stem.io.ConsequentWriter;
import org.stem.io.FatFileReader;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.CRC32;

/**
 * Fat file format:
 * <p/>
 * Header
 * Header is placed at the end of the fat file.
 * Format:
 * <p/>
 * 0        1
 * +--------+
 * | Marker |
 * +--------+
 */
public class FatFile {
    private static final Logger logger = LoggerFactory.getLogger(FatFile.class);
    private DataTracker tracker;

    private final long capacity;

    public FatFileReader getReader() {
        return reader;
    }

    private final FatFileReader reader;
    public final Integer id;
    private final long indexHeaderOffset;
    public ConsequentWriter writer;
    public FatFileIndex index;

    public static final int PAYLOAD_OFFSET = 1;

    public static final int MARKER_BLANK = 0x0;
    public static final int MARKER_ACTIVE = 0x1;
    public static final int MARKER_FULL = 0x1; // it's not a mistake

    private static Pattern namePattern = Pattern.compile(FatFileAllocator.FAT_FILE_NAME_REGEX);

    //Object readLock;
    public ReentrantLock readLock = new ReentrantLock();

    CRC32 crc = new CRC32();
    private int pointer;
    private String path;

    public String getPath() {
        return path;
    }

    public boolean isBlank() {
        return RWState.BLANK == state;
    }

    public boolean isActive() {
        return RWState.ACTIVE == state;
    }

    public boolean isFull() {
        return RWState.FULL == state;
    }

    public void reallocate() throws IOException {
        RWState prevState = state;
        byte[] buf = new byte[65536];
        for (int i = 0; i < buf.length; i++) {
            buf[i] = FatFile.MARKER_BLANK;
        }

        int offset = 0;
        int size = (int) writer.length();
        while (true) {
            int retain = size - offset;
            int len = Math.min(buf.length, retain);
            if (0 == len)
                break;
            writer.seek(offset);
            writer.write(buf, 0, len);
            offset += len;
        }

        state = RWState.BLANK;
        pointer = 0;

        // update DataTracker
        tracker.turnIntoBlank(this.reader.length());
    }

    public static enum RWState {
        ACTIVE,
        BLANK,
        FULL
    }

    private RWState state;

    public RWState getState() {
        return state;
    }

    public static FatFile open(String path, DataTracker tracker) throws IOException {
        // TODO: rebuild index
        FatFile file = new FatFile(path, tracker);
        file.init();
        return file;
    }

    /**
     * Read first byte, last byte and assign a state to file
     *
     * @throws IOException
     */
    private void init() throws IOException {
        this.reader.seek(0);
        int firstByte = this.reader.readUnsignedByte();
        if (MARKER_BLANK == firstByte) {
            state = RWState.BLANK;
            pointer = 0;
            tracker.incBlankFatFiles(capacity);

        } else if (MARKER_ACTIVE == firstByte) {
            this.reader.seek(reader.length() - 1);
            int lastByte = this.reader.readUnsignedByte();
            if (MARKER_FULL == lastByte) {
                state = RWState.FULL;
                loadIndex();
                tracker.incFullFatFiles(capacity);

            } else if (MARKER_BLANK == lastByte) {
                state = RWState.ACTIVE;
                pointer = reconsturctIndex();
                tracker.setActiveFatFile(capacity);
            } else {
                throw new IOException(String.format("File %s is corrupted. Last byte value: %s", path, lastByte));
            }
        } else {
            throw new IOException(String.format("File %s is corrupted. First byte value: %s", path, firstByte));
        }
    }

    private void loadIndex() throws IOException {
        FileChannel channel = this.reader.getChannel();
        try {
            index = FatFileIndex.deserialize(channel, indexHeaderOffset);
            tracker.count(index);
        } catch (IOException e) {
            throw new IOException("Can't load index for #" + id, e);
        }
    }

    /**
     * This one reconstructs index
     *
     * @return
     * @throws IOException
     */
    private int reconsturctIndex() throws IOException {
        // TODO: iterate through blobs and find what the hell is it
        FileChannel channel = reader.getChannel();

        int offset = 1;
        FatFileIndex index = new FatFileIndex();
        while (true) {
            Blob.Header header = Blob.Header.deserialize(channel, offset);

            if (!header.valid())
                break;

            // build index entry
            FatFileIndex.Entry entry = header.toIndexEntry(offset);
            index.add(entry);
            offset += Blob.Header.SIZE + header.length;

            tracker.count(header);
        }

        this.index = index;

        return offset;
    }

    protected FatFile(String path, DataTracker tracker) throws IOException {
        this.path = path;
        this.tracker = tracker;

        File file = new File(path);
        String name = file.getName();
        Matcher m = namePattern.matcher(name);
        m.find();
        this.id = Integer.valueOf(m.group(1));

        try {
            this.writer = new ConsequentWriter(path);
            this.reader = new FatFileReader(path);
            this.capacity = writer.length();
            this.indexHeaderOffset = capacity - 1 - FatFileIndex.Header.SIZE;

        } catch (FileNotFoundException e) {
            throw new IOException("Can open fat file: ", e);
        }

        index = new FatFileIndex();
    }

    public void writeActiveMarker() throws IOException {
        writer.writeByte(MARKER_ACTIVE);
        state = RWState.ACTIVE;
        pointer += 1; // TODO: change to pointer = FatFile.PAYLOAD_OFFSET
    }

    public void markActive() throws IOException {
        writer.seek(0);
        writer.writeByte(MARKER_ACTIVE);
        state = RWState.ACTIVE;
        pointer = 1;
        //pointer += 1; // TODO: change to pointer = FatFile.PAYLOAD_OFFSET
    }

    long getWritePointer() throws IOException {
        return writer.getFilePointer();
    }

    public void writeFullMarker() throws IOException {
        writer.writeByte(MARKER_FULL);
        state = RWState.FULL;
        pointer += 1;
    }

    public boolean hasSpaceFor(Blob blob) {
        return hasSpaceFor(blob.size());
    }

    public boolean hasSpaceFor(int blobSize) {
        int blobTotalSize = Blob.Header.SIZE + blobSize;
        return getFreeSpace() >= blobTotalSize + FatFileIndex.Entry.SIZE;
    }

    public long size() {
        return capacity;
    }

    public long getFreeSpace() {
        return capacity - pointer - index.getSize();
    }

    public void writeBlob(String key, byte[] body) throws IOException {
        try {
            writeBlob(Hex.decodeHex(key.toCharArray()), ByteBuffer.wrap(body));
        } catch (DecoderException e) {
            throw new RuntimeException(e);
        }
    }

    public BlobDescriptor writeBlob(Blob blob) throws IOException {
        // TODO: we already have Blob.Header here, we should avoid building it in further step
        return writeBlob(blob.key(), blob.data());
    }

    public BlobDescriptor writeBlob(byte[] key, byte[] body) throws IOException {
        // TODO: update DataTracker numbers
        return writeBlob(key, ByteBuffer.wrap(body));
    }

    // TODO: utilize FileChannel
    public synchronized BlobDescriptor writeBlob(byte[] key, ByteBuffer blob) throws IOException {
        //int offset = (int) writer.getFilePointer(); // TODO: This is WRONG, don't know why
        int offset = pointer;
        int length = blob.capacity();
        Blob.Header header = Blob.Header.create(key, length, FatFileIndex.Entry.FLAG_LIVE);

        writer.seek(pointer);
        writer.write(header);
        writer.write(blob.array()); // payload

        int bodyOffset = offset + Blob.Header.SIZE;
        pointer = bodyOffset + length;

        FatFileIndex.Entry indexEntry = FatFileIndex.create(header, offset, FatFileIndex.Entry.FLAG_LIVE);
        index.add(indexEntry);

        tracker.count(header);

        return new BlobDescriptor(this.id, offset, bodyOffset);
    }

    public byte[] readBlob(Integer offset, Integer length) throws IOException {
        readLock.lock();

        try {
            byte[] data = new byte[length];
            reader.seek(offset);
            reader.read(data);
            return data;
        }
        finally {
            readLock.unlock();
        }
    }

    public byte[] deleteBlob(Integer bodyOffset) throws IOException {
        readLock.lock();

        try {
            long headerOffset = bodyOffset - Blob.Header.SIZE;
            if (headerOffset < 1)
                throw new IOException(String.format("Invalid blob offset: %s", headerOffset));

            Blob.Header header = Blob.Header.deserialize(this.reader.getChannel(), headerOffset);
            boolean alreadyDeleted = header.isDeleted();

            if (!header.valid())
                throw new IOException(String.format("Blob located by offset %s (%s) is corrupted (may be offset is invalid)", bodyOffset, headerOffset));

            header.deleteFlag = FatFileIndex.Entry.FLAG_DELETED;
            writer.seek(headerOffset); //
            writer.write(header);

            FatFileIndex.Entry indexEntry = index.findByKey(header.key);
            indexEntry.deleteFlag = FatFileIndex.Entry.FLAG_DELETED;

            if (isFull())
                writeIndex();

            if (!alreadyDeleted) {
                tracker.discount(header.key, header.length, this.id);
            }

            return header.key;
        }
        finally {
            readLock.unlock();
        }
    }

    public void writeIndex() throws IOException {
        ByteBuffer indexSerialized = index.serialize();

        writer.seek(capacity - indexSerialized.capacity() - 1);
        writer.write(indexSerialized.array());

        pointer = (int) writer.getFilePointer();
    }

    public void close() {
        try {
            reader.close();
            writer.close();
        } catch (IOException e) {
            logger.error("Can't close {}, {}", reader, writer);

        }
    }

    public void seek(int position) throws IOException {
        writer.seek(position);
        pointer = position;
    }

    public int getPointer() {
        return pointer;
    }

    @Override
    public String toString() {
        return "FatFile{" +
                "id=" + id +
                ", state=" + state +
                '}';
    }
}
