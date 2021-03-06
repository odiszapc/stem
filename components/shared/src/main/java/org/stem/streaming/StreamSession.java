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

import java.util.Map;
import java.util.UUID;

public class StreamSession extends AbstractSession {

    String endpoint;
    Map<UUID, DiskMovement> movements; // session covers many disks

    public StreamSession() {
    }

    public StreamSession(String endpoint, Map<UUID, DiskMovement> movements) {
        this.endpoint = endpoint;
        this.movements = movements;
    }

    @Override
    public String name() {
        return endpoint;
    }

    @Override
    protected Object formingEntity() {
        return this;
    }

    //public Map<Long, BucketStreamingPart> getDiskMovements

    public String getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    public Map<UUID, DiskMovement> getMovements() {
        return movements;
    }

    public void setMovements(Map<UUID, DiskMovement> movements) {
        this.movements = movements;
    }
}
