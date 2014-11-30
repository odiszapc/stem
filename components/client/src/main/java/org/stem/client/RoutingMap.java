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

import java.net.InetSocketAddress;
import java.util.*;

public class RoutingMap {

    private Map<UUID, InetSocketAddress> diskAddresses = new HashMap<>();
    private Map<Long, Set<UUID>> partitionsMap = new HashMap<>();

    public RoutingMap(Map<UUID, InetSocketAddress> disks, Map<Long, Set<UUID>> partitions) {
        this.diskAddresses = disks;
        this.partitionsMap = partitions;
    }

    public RoutingMap() {
    }

    public InetSocketAddress getDiskAddress(UUID id) {
        return diskAddresses.get(id);
    }

    public Set<InetSocketAddress> getEndpoints(Long partition) {
        Set<UUID> disks = partitionsMap.get(partition);
        Set<InetSocketAddress> addresses = new HashSet<>(disks.size());
        for (UUID disk : disks)
            addresses.add(diskAddresses.get(disk));
        return addresses;
    }
}
