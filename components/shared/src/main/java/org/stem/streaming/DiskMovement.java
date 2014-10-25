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

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

public class DiskMovement {
    UUID diskId;
    AtomicLong completed;
    AtomicLong total;

    // TODO: currentIndex to make it possible to resume the session

    @JsonIgnore
    private StreamSession session; // Let's do a simple link
    Map<Long, BucketStreamingPart> bucketStreams = new HashMap<Long, BucketStreamingPart>();

    public void setSession(StreamSession session) {
        this.session = session;
    }

    public DiskMovement() {
        completed = new AtomicLong(-1);
        total = new AtomicLong(-1);
    }

    public DiskMovement(UUID diskId) {
        this();
        this.diskId = diskId;
    }

    public void put(Long vBucket, BucketStreamingPart part) {
        bucketStreams.put(vBucket, part);
    }

    public BucketStreamingPart get(Long vBucket) {
        return bucketStreams.get(vBucket);
    }

    public UUID getDiskId() {
        return diskId;
    }

    public void setDiskId(UUID diskId) {
        this.diskId = diskId;
    }

    public Map<Long, BucketStreamingPart> getBucketStreams() {
        return bucketStreams;
    }

    public void setBucketStreams(Map<Long, BucketStreamingPart> bucketStreams) {
        this.bucketStreams = bucketStreams;
    }


    // TODO: isn't it a part of interface like Progressable?
    @JsonIgnore
    public boolean isInitialized() {
        return completed.get() != -1 && total.get() != -1;
    }

    public void progress(long delta) {
        completed.addAndGet(delta);
        if (null != session) {
            session.progress(delta);
        }
    }

    public void setProgress(long value) {
        long before = completed.get();
        completed.set(value);
        if (null != session) {
            session.progress(-before);
            session.progress(completed.get());
        }
    }

    public long getCompleted() {
        return completed.get();
    }

    public long getTotal() {
        return total.get();
    }

    public void setTotal(long value) {
        long before = total.get();
        total.set(value);
        if (null != session) {
            session.addTotal(-before);
            session.addTotal(total.get());
        }
    }

    public void init(StreamSession session, int progress, long total) {
        setSession(session);
        setProgress(progress);
        setTotal(total);
    }
}
