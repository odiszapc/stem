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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;

public class BBUtils {

    public static byte[] hexStringToByteArray(String s) {
        int len = s.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
                    + Character.digit(s.charAt(i + 1), 16));
        }
        return data;
    }

    public static File getDirectory(String directoryPath) throws IOException {
        File dir = new File(directoryPath);
        if (!dir.exists()) {
            throw new IOException(String.format("Directory does not exist: %s", directoryPath));
        }

        if (!dir.isDirectory()) {
            throw new IOException(String.format("Path is not a regular directory: %s", directoryPath));
        }
        return dir;
    }

    public static String readString(ByteBuf buf) {
        int length = buf.readUnsignedShort();
        byte[] bytes = new byte[length];
        buf.readBytes(bytes, 0, length);
        //String str = buf.toString(buf.readerIndex(), length, CharsetUtil.UTF_8);

        //return buf.toString(buf.readerIndex(), length, CharsetUtil.UTF_8);
        return new String(bytes, CharsetUtil.UTF_8);
    }

    public static void writeString(String str, ByteBuf buf) {
        ByteBuf strBuf = stringToBB(str);
        buf.writeBytes(strBuf);
    }

    public static ByteBuf stringToBB(String str) {
        ByteBuf bytes = stringBytes(str);
        return Unpooled.wrappedBuffer(shortToBB(bytes.readableBytes()), bytes);
    }

    public static int sizeOfString(String str) {
        return 2 + str.getBytes(CharsetUtil.UTF_8).length;
    }

    public static void writeUuid(UUID value, ByteBuf dest) {
        dest
                .writeLong(value.getMostSignificantBits())
                .writeLong(value.getLeastSignificantBits());
    }

    public static UUID readUuid(ByteBuf in) {
        long mostBits = in.readLong();
        long leastBits = in.readLong();
        return new UUID(mostBits, leastBits);
    }

    public static ByteBuf intToBB(int value) {
        return Unpooled.buffer(4).writeInt(value);
    }

    public static ByteBuf shortToBB(int value) {
        return Unpooled.buffer(2).writeShort(value);
    }

    public static ByteBuf stringBytes(String str) {
        return Unpooled.wrappedBuffer(str.getBytes(CharsetUtil.UTF_8));

    }

    public static ByteBuffer fromInt(int value) {
        ByteBuffer buf = ByteBuffer.allocate(4);
        buf.putInt(value);
        return buf;
    }

    public static void writeBytes(byte[] bytes, ByteBuf buf) {
        buf.writeBytes(bytes);
    }
}
