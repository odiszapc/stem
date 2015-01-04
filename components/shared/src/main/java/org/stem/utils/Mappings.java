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

package org.stem.utils;

import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.stem.api.REST;

import java.util.*;

public abstract class Mappings {

    public static final int UUID_PACKED_SIZE = 16;

    public static class Encoder {

        private final REST.Mapping mapping;
        private final NumberFormat formatter;
        private final ArrayList<Long> buckets; // buckets numbers sorted
        private final int numberOfBuckets;
        private final Map<Long, UUID> diskMap;
        private final Map<UUID, Long> invertedDiskMap;
        private final int rf;

        public Encoder(REST.Mapping mapping) {
            this.mapping = mapping;

            // We decide to treat buckets numbers as a monotonic sequence without gaps
            buckets = Lists.newArrayList(mapping.getBuckets());
            Collections.sort(buckets);
            numberOfBuckets = buckets.size();

            rf = reconstructReplicationFactor();

            // List of unique disks
            List<REST.Disk> disks = Lists.newArrayList(mapping.getDisks());
            formatter = prepareFormat(disks.size());
            diskMap = prepareDiskMap(disks);

            invertedDiskMap = new HashMap<>(diskMap.size());
            for (Map.Entry<Long, UUID> entry : diskMap.entrySet())
                invertedDiskMap.put(entry.getValue(), entry.getKey());
        }

        private int reconstructReplicationFactor() {
            int rf = -1;
            for (REST.ReplicaSet replicaSet : mapping.getAllReplicas()) {
                int replicas = replicaSet.getReplicas().size();
                if (rf == -1)
                    rf = replicas;
                else if (rf != replicas)
                    throw new RuntimeException(String.format("Corrupted mappings, rf(%s) != replicas(%s)", rf, replicas));
            }

            if (rf < 0 && !mapping.isEmpty())
                throw new RuntimeException("mappings corrupted");

            return rf;
        }

        private Map<Long, UUID> prepareDiskMap(List<REST.Disk> disks) { // TODO: We dont support more then 2^32 disks
            Map<Long, UUID> result = new HashMap<>(disks.size());
            for (int i = 0; i < disks.size(); i++)
                result.put((long) i, disks.get(i).getId());

            return result;
        }

        public byte[] encode() {
            if (mapping.isEmpty())
                return new byte[]{};

            final int diskMapSize = diskMap.size() * (formatter.granularityInBytes + UUID_PACKED_SIZE);
            final int bucketsSize = rf * formatter.granularityInBytes * numberOfBuckets;
            final int bufferSize = Header.PACKED_SIZE + diskMapSize + bucketsSize;
            ByteBuf buf = Unpooled.buffer(bufferSize);

            Header header = new Header(numberOfBuckets, rf, formatter);
            writeHeader(header, buf);

            writeDiskMap(buf);

            writeBuckets(buf);

            return buf.nioBuffer().array();
        }

        private void writeHeader(Header header, ByteBuf buf) {
            header.writeTo(buf);
        }

        private void writeDiskMap(ByteBuf buf) {
            List<Long> indexes = Lists.newArrayList(diskMap.keySet());
            Collections.sort(indexes);

            buf.writeLong((long) indexes.size());
            for (Long ident : indexes) {
                formatter.codec.write(ident, buf);
                BBUtils.writeUuid(diskMap.get(ident), buf);
            }
        }

        private void writeBuckets(ByteBuf buf) {
            for (long bucket : buckets) {
                for (REST.Disk disk : mapping.getReplicas(bucket)) {
                    long ident = invertedDiskMap.get(disk.getId());
                    formatter.codec.write(ident, buf);
                }
            }
        }

        public static NumberFormat prepareFormat(long maxBucketNumber) {
            final int longSize = 64;
            int cap = longSize - Long.numberOfLeadingZeros(maxBucketNumber);
            return NumberFormat.minimalFormat(cap);
        }
    }

    public static class Decoder {

        final ByteBuf buf;

        public Decoder(byte[] data) {
            buf = Unpooled.wrappedBuffer(data);
        }

        public REST.Mapping decode() {
            // TODO: add validation

            if (!buf.isReadable())
                return new REST.Mapping();

            Header header = readHeader(buf);
            Map<Long, UUID> diskMap = readDiskMap(header, buf);

            return buildMapping(header, diskMap, buf);
        }

        private static REST.Mapping buildMapping(Header header, Map<Long, UUID> diskMap, ByteBuf buf) {
            List<REST.Disk> diskCache = new ArrayList<>();
            List<REST.ReplicaSet> replicaSetCache = new ArrayList<>();

            REST.Mapping result = new REST.Mapping();
            Map<Long, REST.ReplicaSet> dataMap = result.getMap();
            for (long bucket = 0; bucket < header.buckets; bucket++) {
                Set<REST.Disk> disks = new HashSet<>(header.rf);
                for (int j = 0; j < header.rf; j++) {
                    Long index = header.formatter.codec.read(buf);
                    UUID ident = diskMap.get(index);
                    REST.Disk disk = createMaybeCache(ident, diskCache);
                    disks.add(disk);
                }

                REST.ReplicaSet replicaSet = createMaybeCache(disks, replicaSetCache);
                dataMap.put(bucket, replicaSet);
            }

            return result;
        }

        private static REST.ReplicaSet createMaybeCache(Set<REST.Disk> disks, List<REST.ReplicaSet> cache) {
            REST.ReplicaSet packed = new REST.ReplicaSet();
            packed.addDisks(disks);

            if (!cache.contains(packed))
                cache.add(packed);
            else {
                int index = cache.indexOf(packed);
                packed = cache.get(index);
            }
            return packed;
        }

        private static REST.Disk createMaybeCache(UUID ident, List<REST.Disk> diskCache) {
            REST.Disk packed = new REST.Disk(ident, "", -1, -1);
            if (!diskCache.contains(packed)) {
                diskCache.add(packed);
            } else {
                int index = diskCache.indexOf(packed);
                packed = diskCache.get(index);
            }
            return packed;

        }

        private Map<Long, UUID> readDiskMap(Header header, ByteBuf buf) {

            long size = buf.readLong();
            Map<Long, UUID> map = new HashMap<>((int) size);
            for (int i = 0; i < size; i++) {
                Long index = header.formatter.codec.read(buf);
                UUID ident = BBUtils.readUuid(buf);
                map.put(index, ident);
            }

            return map;
        }

        private Header readHeader(ByteBuf buf) {
            return Header.create(buf);
        }
    }

    private static class Header {

        private final long buckets;
        private int rf;
        private final NumberFormat formatter;
        private static final int PACKED_SIZE = 16;

        public Header(long buckets, int rf, NumberFormat formatter) {
            this.buckets = buckets;
            this.rf = rf;
            this.formatter = formatter;
        }

        public void writeTo(ByteBuf buf) {
            buf.writeLong(buckets);
            buf.writeInt(rf);
            buf.writeInt(formatter.granularityInBytes);
        }

        public static Header create(ByteBuf buf) {
            long buckets = buf.readLong();
            int rf = buf.readInt();
            int numberCapacity = buf.readInt();

            return new Header(buckets, rf, NumberFormat.fromSize(numberCapacity));
        }
    }

    public static enum NumberFormat {
        SHORT(2, new ShortCodec()),
        INT(4, new IntCodec()),
        LONG(8, new LongCodec());

        public static NumberFormat minimalFormat(int bits) {
            if (bits <= 16) return SHORT;
            else if (bits <= 32) return INT;
            else return LONG;
        }

        public static NumberFormat fromSize(int sizeInBytes) {
            return minimalFormat(sizeInBytes * 8);
        }

        private final int granularityInBytes; // TODO: rename
        private final NumberCodec codec;

        NumberFormat(int size, NumberCodec codec) {
            this.granularityInBytes = size;
            this.codec = codec;
        }
    }

    private interface NumberCodec {

        Long read(ByteBuf buf);
        void write(Long value, ByteBuf buf);
    }

    private static class ShortCodec implements NumberCodec {

        @Override
        public Long read(ByteBuf buf) {
            return (long) buf.readShort();
        }

        @Override
        public void write(Long value, ByteBuf buf) {
            buf.writeShort(value.shortValue());
        }
    }

    private static class IntCodec implements NumberCodec {

        @Override
        public Long read(ByteBuf buf) {
            return (long) buf.readInt();
        }

        @Override
        public void write(Long value, ByteBuf buf) {
            buf.writeShort(value.intValue());
        }
    }

    private static class LongCodec implements NumberCodec {

        @Override
        public Long read(ByteBuf buf) {
            return buf.readLong();
        }

        @Override
        public void write(Long value, ByteBuf buf) {
            buf.writeLong(value);
        }
    }
}
