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

package org.stem.client;

import org.apache.commons.codec.binary.Hex;

public class Blob {
    public final byte[] key;
    public final byte[] body;

    public static Blob create(byte[] key, byte[] body) {
        return new Blob(key, body);
    }

    private Blob(byte[] key, byte[] body) {
        this.key = key;
        this.body = body;
    }

    public int getBlobSize() {
        return body.length;
    }

    @Override
    public String toString() {

        return String.format("Blob{key = %s, length = %s}", new String(Hex.encodeHex(key)), getBlobSize());
    }
}
