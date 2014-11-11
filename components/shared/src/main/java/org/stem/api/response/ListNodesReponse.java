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

package org.stem.api.response;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class ListNodesReponse extends StemResponse {


    public ListNodesReponse() {
    }

    public static class StorageNode {

        UUID id;
        String hostname;
        String listen;
        Long capacity;
        List<Disk> disks = new ArrayList<>();


        public List<Disk> getDisks() {
            return disks;
        }

        public StorageNode() {
        }

        public StorageNode(UUID id, String hostname, String listen, Long capacity) {
            this.id = id;
            this.hostname = hostname;
            this.listen = listen;
            this.capacity = capacity;
        }
    }

    public static class Disk {

        UUID id;
        String path;
        long total;
        long used;

        public Disk() {
        }

        public Disk(UUID id, String path, long total, long used) {
            this.id = id;
            this.path = path;
            this.total = total;
            this.used = used;
        }
    }
}
