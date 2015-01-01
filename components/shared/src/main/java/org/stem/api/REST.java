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
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.*;
import org.stem.api.request.ClusterConfiguration;
import org.stem.coordination.ZNode;
import org.stem.coordination.ZNodeAbstract;
import org.stem.coordination.ZookeeperPaths;
import org.stem.utils.BBUtils;
import org.stem.utils.JsonUtils;
import org.stem.utils.Mappings;
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
        ClusterConfiguration configuration;
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
                    ", configuration=" + configuration.toString() +
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
            return ZookeeperPaths.CURRENT_TOPOLOGY;
        }

        public Set<StorageNode> nodes() {
            Set<StorageNode> result = new HashSet<>();
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

        public static final ZNode.Codec CODEC = new ZNode.Codec() {

            @Override
            public byte[] encode(Object obj) {
                return new Mappings.Encoder((Mapping) obj).encode();
            }

            @Override
            public <T extends ZNode> T decode(byte[] raw, Class<T> clazz) {
                return (T) new Mappings.Decoder(raw).decode();
            }
        };

        @JsonIgnore
        @Override
        protected ZNode.Codec codec() {
            return CODEC;
        }

        @JsonIgnore
        private String name;

        private final Map<Long, ReplicaSet> map = new HashMap<>(); // TODO: pack to Map<Long, Set<UUID> >

        @JsonIgnore
        public List<Long> getBuckets() {
            return Lists.newArrayList(map.keySet());
        }

        @JsonIgnore
        public ReplicaSet getReplicaSet(Long bucket) {
            return map.get(bucket);
        }

        @JsonIgnore
        public Set<Disk> getReplicas(Long bucket) {
            return map.get(bucket).getReplicas();
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

        public long size() {
            return map.size();
        }

        @JsonIgnore
        public Set<Disk> getDisks() {
            Set<Disk> result = new HashSet<>();
            for (ReplicaSet set : getAllReplicas())
                result.addAll(set.getReplicas());
            return result;
        }

        @JsonIgnore
        public boolean isEmpty() {
            return map.isEmpty();
        }
    }

    @EqualsAndHashCode(callSuper = false)
    @Data
    @RequiredArgsConstructor
    public static class TopologySnapshot extends ZNodeAbstract {

        public static final ZNode.Codec CODEC = new ZNode.Codec() {

            @Override
            public byte[] encode(Object obj) {
                String topologyPacked = JsonUtils.encode(((TopologySnapshot)obj).topology);
                byte[] mappingPacked = new Mappings.Encoder(((TopologySnapshot) obj).mapping).encode();

                ByteBuf buffer = Unpooled.buffer();
                BBUtils.writeString(topologyPacked, buffer);
                BBUtils.writeBytes(mappingPacked, buffer);

                byte[] result = new byte[buffer.readableBytes()];
                buffer.readBytes(result);
                return result;
            }

            @Override
            public <T extends ZNode> T decode(byte[] raw, Class<T> clazz) {
                ByteBuf buf = Unpooled.wrappedBuffer(raw);
                String topologyPacked = BBUtils.readString(buf);
                Topology topology = JsonUtils.decode(topologyPacked, Topology.class);

                byte[] mappingRaw = new byte[buf.readableBytes()];
                buf.readBytes(mappingRaw);
                Mapping mapping = new Mappings.Decoder(mappingRaw).decode();
                return (T) new TopologySnapshot(topology, mapping);
            }
        };

        @Override
        protected Codec codec() {
            return CODEC;
        }

        private final Topology topology;
        private final Mapping mapping;

        @Override
        public String name() {
            return ZookeeperPaths.TOPOLOGY_SNAPSHOT;
        }
    }

    @Data
    @RequiredArgsConstructor
    @EqualsAndHashCode(of = {"replicas"})
    public static class ReplicaSet {

        private final Set<Disk> replicas = new HashSet<>();

        public void addDisk(Disk disk) {
            replicas.add(disk);
        }

        public void addDisks(Set<Disk> disks) {
            replicas.addAll(disks);
        }
    }
}
