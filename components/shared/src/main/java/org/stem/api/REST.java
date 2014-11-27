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

package org.stem.api;

import com.fasterxml.jackson.annotation.JsonIdentityInfo;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.ObjectIdGenerators;
import com.google.common.collect.Lists;
import lombok.*;
import org.stem.coordination.ZNodeAbstract;
import org.stem.coordination.ZookeeperPaths;
import org.stem.utils.Utils;

import java.net.InetSocketAddress;
import java.util.*;

/**
 * Type that mirroring existing ones in Cluster or Topology core
 * and used to transfer themselves through network - through REST or Zookeeper
 */
public abstract class REST {

    @Data
    public static class Cluster {

        String name;
        int vBucketsNum;
        int rf;
        String partitioner;
        String zookeeperEndpoint;
        String[] metaStoreContactPoints;
        long usedBytes;
        long totalBytes;

        List<StorageNode> nodes = new ArrayList<>();

        public List<StorageNode> getNodes() {
            return nodes;
        }

        public void setNodes(List<StorageNode> nodes) {
            this.nodes = nodes;
        }

        @Override
        public String toString() {
            return "Cluster{" +
                    "name='" + name + '\'' +
                    ", vBucketsNum=" + vBucketsNum +
                    ", rf=" + rf +
                    ", partitioner='" + partitioner + '\'' +
                    ", zookeeperEndpoint='" + zookeeperEndpoint + '\'' +
                    ", metaStoreContactPoints=" + Arrays.toString(metaStoreContactPoints) +
                    '}';
        }
    }

    @EqualsAndHashCode(callSuper = false)
    @Data
    @RequiredArgsConstructor
    public static class Topology extends ZNodeAbstract {

        final List<Datacenter> dataCenters = new ArrayList<>();

        @JsonIgnore
        @Override
        public String name() {
            return ZookeeperPaths.CLUSTER_TOPOLOGY;
        }

        public List<StorageNode> nodes() {
            List<StorageNode> result = new ArrayList<>();
            for (Datacenter dataCenter : dataCenters) {
                for (Rack rack : dataCenter.getRacks()) {
                    for (StorageNode node : rack.getNodes()) {
                        result.add(node);
                    }
                }
            }
            return result;
        }
    }

    @Data
    @RequiredArgsConstructor
    @NoArgsConstructor
    @EqualsAndHashCode(of = {"id"})
    public static class Datacenter {

        @NonNull UUID id;
        @NonNull String name;
        final List<Rack> racks = new ArrayList<>();
    }

    @Data
    @RequiredArgsConstructor
    @NoArgsConstructor
    @EqualsAndHashCode(of = {"id"})
    public static class Rack {

        @NonNull UUID id;
        @NonNull String name;
        final List<StorageNode> nodes = new ArrayList<>();
    }

    @Data
    @RequiredArgsConstructor
    @NoArgsConstructor
    @EqualsAndHashCode(of = {"id"}, callSuper = false)
    public static class StorageNode extends ZNodeAbstract {

        @NonNull UUID id;
        @NonNull String hostname;
        @NonNull String listen;
        @NonNull Long capacity;

        final List<String> ipAddresses = new ArrayList<String>();
        final List<Disk> disks = new ArrayList<>();

        @JsonIgnore
        public String getListenHost() {
            return Utils.getHost(listen);
        }

        @JsonIgnore
        public int getListenPort() {
            return Utils.getPort(listen);
        }

        public void setListen(String host, int port) {
            this.listen = host + ':' + port;
        }

        @JsonIgnore
        public InetSocketAddress getSocketAddress() {
            return Utils.normalizeSocketAddr(new InetSocketAddress(getListenHost(), getListenPort()));
        }

        @Override
        public String name() {
            return id.toString();
        }
    }

    @Data
    @RequiredArgsConstructor
    @NoArgsConstructor
    @EqualsAndHashCode(of = {"id"})
    @JsonIdentityInfo(property = "@", generator = ObjectIdGenerators.IntSequenceGenerator.class)
    public static class Disk {

        @NonNull UUID id;
        @NonNull String path;
        @NonNull long used;
        @NonNull long total;
    }

    @EqualsAndHashCode(callSuper = false)
    @Data
    @RequiredArgsConstructor
    public static class Mapping extends ZNodeAbstract {

        @JsonIgnore
        private String name;

        private final Map<Long, ReplicaSet> map = new HashMap<>(); // TODO: pack to Map<Long, Set<UUID> >

        @JsonIgnore
        public List<Long> getBuckets() {
            return Lists.newArrayList(map.keySet());
        }

        @JsonIgnore
        public ReplicaSet getReplicas(Long bucket) {
            return map.get(bucket);
        }

        @JsonIgnore
        public Collection<ReplicaSet> getAllReplicas() {
            return map.values();
        }

        public void setName(String name) {
            this.name = name;
        }

        @Override
        public String name() {
            return null != name ? name : ZookeeperPaths.MAPPING;
        }
    }

    @EqualsAndHashCode(callSuper = false)
    @Data
    @RequiredArgsConstructor
    public static class TopologySnapshot extends ZNodeAbstract {

        private final Topology topology;
        private final Mapping mapping;

        @Override
        public String name() {
            return ZookeeperPaths.TOPOLOGY_SNAPSHOT;
        }
    }


    @Data
    @RequiredArgsConstructor
    public static class ReplicaSet {

        private final Set<Disk> replicas = new HashSet<>();

        public void addDisk(Disk disk) {
            replicas.add(disk);
        }
    }
}
