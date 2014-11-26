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

import org.stem.api.REST;
import org.stem.domain.ArrayBalancer;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

public class Metadata {

    private StemCluster.Manager cluster;
    private REST.Cluster descriptor;
    private final AtomicReference<REST.Topology> topology = new AtomicReference<>();
    private ArrayBalancer hashTable;

    private final AtomicReference<REST.Mapping> mapping = new AtomicReference<>();

    private final ConcurrentMap<InetSocketAddress, Host> hosts = new ConcurrentHashMap<InetSocketAddress, Host>();
    //private final ConcurrentMap<UUID, InetSocketAddress> diskHostAddresses = new ConcurrentHashMap<>();
    private final AtomicReference<Map<UUID, InetSocketAddress>> diskHostAddresses = new AtomicReference<>();


    //private final ConcurrentMap<REST.Disk, REST.StorageNode> disks = new ConcurrentHashMap<>();


    public Metadata(StemCluster.Manager cluster) {
        this.cluster = cluster;
    }

    Collection<Host> allHosts() {
        return hosts.values();
    }

    public Host getHost(InetSocketAddress addr) {
        return hosts.get(addr);
    }

    public Host add(InetSocketAddress addr) {
        Host newHost = new Host(addr);
        Host previous = hosts.putIfAbsent(addr, newHost);
        return null == previous ? newHost : null;
    }

    public boolean remove(Host host) {
        return hosts.remove(host.getSocketAddress()) != null;
    }

    public void setDescriptor(REST.Cluster descriptor) {
        this.descriptor = descriptor;
    }

    public REST.Topology getTopology() {
        return topology.get();
    }

    public void setTopology(REST.Topology topology) {
        REST.Topology previous = this.topology.getAndSet(topology);
    }


    public void setMapping(REST.Mapping mapping) { // TODO: what if mapping will be received first

        REST.Topology topology = this.topology.get();
        if (null == topology) { // topology update has not been received yet
            this.mapping.getAndSet(mapping);
            return;
        }

        updateDiskHostsAddressesMap(mapping, topology);

        this.mapping.getAndSet(mapping);
    }

    private void updateDiskHostsAddressesMap(REST.Mapping mapping, REST.Topology topology) {
        Map<UUID, InetSocketAddress> cache = buildDiskHostsAddressesMap(topology);
        this.diskHostAddresses.getAndSet(cache);
    }

    private Map<UUID, InetSocketAddress> buildDiskHostsAddressesMap(REST.Topology topology) {
        Map<UUID, InetSocketAddress> result = new HashMap<>();
        for (REST.StorageNode node : topology.nodes()) {
            for (REST.Disk disk : node.getDisks()) {
                result.put(disk.getId(), node.getSocketAddress());
            }
        }

        return result;
    }

    public REST.Mapping getMapping() {
        return mapping.get();
    }

    public Host findHostForDisk(UUID disk) {
        return null;
    }
}
