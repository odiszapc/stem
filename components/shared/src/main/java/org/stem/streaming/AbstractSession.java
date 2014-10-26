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

package org.stem.streaming;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.stem.coordination.ZNodeAbstract;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

public abstract class AbstractSession extends ZNodeAbstract {

    protected final UUID sessionId;
    protected final AtomicLong completed;
    protected final AtomicLong total;

    protected AbstractSession() {
        sessionId = UUID.randomUUID();
        completed = new AtomicLong(-1);
        total = new AtomicLong(-1);
    }

    protected AbstractSession(UUID sessionId, AtomicLong total) {
        this.sessionId = sessionId;
        this.total = total;
        completed = new AtomicLong();
    }

    @JsonIgnore
    public boolean isInitialized() {
        return completed.get() != -1 && total.get() != -1;
    }

    public void progress(long delta) {
        completed.addAndGet(delta);
    }

    public void setProgress(long value) {
        completed.set(value);
    }

    public long getCompleted() {
        return completed.get();
    }

    public long getTotal() {
        return total.get();
    }

    public void setTotal(long value) {
        total.set(value);
    }

    public void addTotal(long value) {
        total.addAndGet(value);
    }
}
