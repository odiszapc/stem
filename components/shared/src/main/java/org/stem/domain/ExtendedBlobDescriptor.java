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

package org.stem.domain;

import java.util.UUID;

public class ExtendedBlobDescriptor extends BlobDescriptor {

    final byte[] key;
    final int length;
    UUID disk = null;

    public ExtendedBlobDescriptor(byte[] key, int length, UUID disk, BlobDescriptor d) {
        this(key, length, disk, d.FFIndex, d.offset, d.bodyOffset);
    }

    public ExtendedBlobDescriptor(byte[] key, int length, UUID disk, int fatFileIndex, int offset, int bodyOffset) {
        super(fatFileIndex, offset, bodyOffset);
        this.key = key;
        this.length = length;
        this.disk = disk;
    }

    public ExtendedBlobDescriptor(byte[] key, int length, int fatFileIndex, int offset, int bodyOffset) {
        super(fatFileIndex, offset, bodyOffset);
        this.key = key;
        this.length = length;
    }

    public byte[] getKey() {
        return key;
    }

    public int getLength() {
        return length;
    }

    public UUID getDisk() {
        return disk;
    }

    public void setDisk(UUID disk) {
        this.disk = disk;
    }

}
