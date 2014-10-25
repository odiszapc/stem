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

package org.stem.db.compaction;

import org.stem.db.Blob;
import org.stem.db.FatFile;
import org.stem.domain.ExtendedBlobDescriptor;

import java.io.IOException;
import java.util.Iterator;

public class FFExtendedScanner implements Iterator<ExtendedBlobDescriptor> {
    private FatFile ff;
    private ExtendedBlobDescriptor currentBlob;
    int nextOffset = FatFile.PAYLOAD_OFFSET;

    public FFExtendedScanner(FatFile ff) {
        this.ff = ff;
        if (!ff.isFull())
            throw new RuntimeException("Non-FULL FatFiles can not be iterated with FFScanner");
    }

    @Override
    public boolean hasNext() {
        ff.readLock.lock();
        try {
            ExtendedBlobDescriptor blob = Blob.deserializeDescriptor(ff, nextOffset);
            if (null != blob) {
                currentBlob = blob;
                nextOffset += Blob.Header.SIZE + blob.getLength();
                return true;
            } else {
                return false;
            }
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        finally {
            ff.readLock.unlock(); // TODO: this is an unreasoned action. Why we lock here!?
        }
    }

    @Override
    public ExtendedBlobDescriptor next() {
        return currentBlob;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}
