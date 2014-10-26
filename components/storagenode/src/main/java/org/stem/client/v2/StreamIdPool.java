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

package org.stem.client.v2;

import org.stem.exceptions.ConnectionBusyException;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;

public class StreamIdPool {

    static final int MAX_STREAMS = 128;
    private final AtomicIntegerArray streamsIds = new AtomicIntegerArray(MAX_STREAMS);
    private final AtomicInteger marked = new AtomicInteger(0);
    private final int HOLD = 1;
    private final int FREE = 0;

    public StreamIdPool() {
        for (int i = 0; i < streamsIds.length(); i++) {
            streamsIds.set(i, FREE);
        }
    }

    public int borrow() throws ConnectionBusyException {
        while (true) {
            int id = getFirstVacant();
            if (-1 == id) {
                throw new ConnectionBusyException();
            }
            if (streamsIds.compareAndSet(id, FREE, HOLD)) {
                return id;
            }
        }
    }

    public void release(int id) {
        assert id < MAX_STREAMS;
        streamsIds.set(id, FREE);
    }

    public void mark() {
        marked.incrementAndGet();
    }

    public void unmark() {
        marked.decrementAndGet();
    }

    public int maxAvailableStreams() {
        return MAX_STREAMS - marked.get();
    }

    private int getFirstVacant() {
        for (int i = 0; i < streamsIds.length(); i++) {
            int state = streamsIds.get(i);
            if (0 == state)
                return i;
        }
        return -1;
    }
}
