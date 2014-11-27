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

public class MappingUtils {

    public static class Encoder {

        private final REST.Mapping mapping;
        private final NumberFormat formatter;
        private final ArrayList<Long> buckets; // buckets numbers sorted
        private final int numberOfBuckets;
        private final Map<Long, UUID> diskMap;

        public Encoder(REST.Mapping mapping) {
            this.mapping = mapping;

            // We decide to treat buckets numbers as a monotonic sequence without gaps
            buckets = Lists.newArrayList(mapping.getBuckets());
            Collections.sort(buckets);
            numberOfBuckets = buckets.size();

            // List of unique disks
            List<REST.Disk> disks = Lists.newArrayList(mapping.getDisks());
            formatter = prepareFormat(disks.size());
            diskMap = prepareDiskMap(disks);
        }

        private Map<Long, UUID> prepareDiskMap(List<REST.Disk> disks) { // TODO: We dont support more then 2^32 disks
            Map<Long, UUID> result = new HashMap<>(disks.size());
            for(int i = 0; i < disks.size(); i++)
                result.put((long)i, disks.get(i).getId());

            return result;
        }

        public byte[] encode() {
            ByteBuf buf = Unpooled.buffer();
            writeHeader(buf);
            writeDiskMap(buf);
            writeBuckets(buf);

            return new byte[]{};
        }

        private void writeBuckets(ByteBuf buf) {
            // TODO:
        }

        private void writeHeader(ByteBuf buf) {
            // TODO:
        }

        private void writeDiskMap(ByteBuf buf) {
            Unpooled.buffer(formatter.sizeInBytes * diskMap.size());
            // TODO:
        }


        public static NumberFormat prepareFormat(long maxBucketNumber) {
            final int longSize = 64;
            int cap = longSize - Long.numberOfLeadingZeros(maxBucketNumber);
            return NumberFormat.minimalFormat(cap);
        }
    }

    public static class Decoder {

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

        private final int sizeInBytes;
        private final NumberCodec codec;

        NumberFormat(int size, NumberCodec codec) {
            this.sizeInBytes = size;
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
