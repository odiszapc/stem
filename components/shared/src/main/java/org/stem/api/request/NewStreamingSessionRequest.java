/*
 * Copyright 2015 Alexey Plotnik
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

package org.stem.api.request;

import java.util.UUID;

public class NewStreamingSessionRequest extends StemRequest {

    UUID source;
    UUID destination;

    Long[] partitions = new Long[]{};

    public NewStreamingSessionRequest() {
    }

    public UUID getSource() {
        return source;
    }

    public void setSource(UUID source) {
        this.source = source;
    }

    public UUID getDestination() {
        return destination;
    }

    public void setDestination(UUID destination) {
        this.destination = destination;
    }

    public Long[] getPartitions() {
        return partitions;
    }

    public void setPartitions(Long[] partitions) {
        this.partitions = partitions;
    }
}
