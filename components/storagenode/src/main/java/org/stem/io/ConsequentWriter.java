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

package org.stem.io;

import org.stem.db.Blob;

import java.io.IOException;
import java.nio.ByteBuffer;

public class ConsequentWriter extends RandomAccessReader {

    public ConsequentWriter(String name) throws IOException {
        this(name, 0);
    }

    public ConsequentWriter(String name, long pos) throws IOException {
        super(name, "rw");
        this.
                seek(pos);
    }

    public void write(Blob.Header header) throws IOException {
        ByteBuffer buf = header.serialize();
        write(buf.array());
    }
}
