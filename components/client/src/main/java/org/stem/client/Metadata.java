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

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

public class Metadata {

    private StemCluster.Manager cluster;
    private REST.Cluster descriptor;
    private final AtomicReference<REST.Topology> topology = new AtomicReference<>();

    private final AtomicReference<REST.Mapping> mapping = new AtomicReference<>(); // TODO: volatile

    private final ConcurrentMap<InetSocketAddress, Host> hosts = new ConcurrentHashMap<InetSocketAddress, Host>();

    private volatile RoutingMap routing = new RoutingMap();

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
        InetSocketAddress address = routing.getDiskAddress(disk);
        Host host = hosts.get(address);
        return host;
    }

    void updateRouting(REST.Topology topology, REST.Mapping mapping) {

        routing = new RoutingMap(buildDiskHostsAddressesMap(topology), simplifyMapping(mapping));
    }

    private Map<Long, Set<UUID>> simplifyMapping(REST.Mapping mapping) {
        Map<Long, Set<UUID>> result = new HashMap<>(mapping.getMap().size());

        List<Set<UUID>> cache = new ArrayList<>();
        for (Map.Entry<Long, REST.ReplicaSet> entry : mapping.getMap().entrySet()) {
            Long partition = entry.getKey();
            Set<UUID> replicas = extractDiskIds(entry.getValue().getReplicas(), cache);
            result.put(partition, replicas);
        }

        return result;
    }

    private Set<UUID> extractDiskIds(Set<REST.Disk> replicas, List<Set<UUID>> cache) {
        Set<UUID> ids = new HashSet<>(replicas.size());
        for (REST.Disk replica : replicas)
            ids.add(replica.getId());

        if (!cache.contains(ids)) {
            cache.add(ids);
        } else {
            int index = cache.indexOf(ids);
            ids = cache.get(index);
        }
        return ids;
    }

    public Set<Host> getHostsForBlob(Blob blob) {
        Set<InetSocketAddress> endpoints = routing.getEndpoints(blob.key);

        Set<Host> replicas = new HashSet<>(endpoints.size());
        for (InetSocketAddress address : endpoints) {
            Host host = getHost(address);
            if (null != host)
                replicas.add(host);
        }

        if (replicas.size() != endpoints.size())
            throw new ClientInternalError(String.format("Inconsistent number of replicas, %s != %s", replicas.size(), endpoints.size()));

        return replicas;
    }

    public Set<UUID> getLocationsForBlob(Blob blob) {
        return routing.getLocations(blob.key);
    }
}
