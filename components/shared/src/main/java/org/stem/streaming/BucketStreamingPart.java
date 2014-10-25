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

import java.util.UUID;

public class BucketStreamingPart {
    long vBucket;
    String endpoint; // Node to stream to
    UUID diskId; // Particular disk to stream to

    // TODO: I think we don't need so precise progress counter (per bucket)
    long completed;
    long total;

    public BucketStreamingPart() {
    }

    public BucketStreamingPart(long vBucket, String endpoint, UUID diskId) {
        this(vBucket, endpoint, diskId, -1, -1);
    }

    public BucketStreamingPart(long vBucket, String endpoint, UUID diskId, long completed, long total) {
        this.vBucket = vBucket;
        this.endpoint = endpoint;
        this.diskId = diskId;
        this.completed = completed;
        this.total = total;
    }

    public long getvBucket() {
        return vBucket;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public UUID getDiskId() {
        return diskId;
    }

    public long getCompleted() {
        return completed;
    }

    public long getTotal() {
        return total;
    }
}
