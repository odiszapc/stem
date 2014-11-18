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

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.*;
import org.stem.coordination.ZNodeAbstract;
import org.stem.coordination.ZookeeperPaths;
import org.stem.utils.Utils;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Type that mirroring existing ones in Cluster or Topology core
 * and used to transfer themselves through network - through REST or Zookeeper
 */
public abstract class REST {

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
    }

    @Data
    @RequiredArgsConstructor
    @NoArgsConstructor
    public static class Datacenter {

        @NonNull UUID id;
        @NonNull String name;
        final List<Rack> racks = new ArrayList<>();
    }

    @Data
    @RequiredArgsConstructor
    @NoArgsConstructor
    public static class Rack {

        @NonNull UUID id;
        @NonNull String name;
        final List<StorageNode> nodes = new ArrayList<>();
    }

    @EqualsAndHashCode(callSuper = false)
    @Data
    @RequiredArgsConstructor
    @NoArgsConstructor
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

        @Override
        public String name() {
            return id.toString();
        }
    }

    @Data
    @RequiredArgsConstructor
    @NoArgsConstructor
    public static class Disk {

        @NonNull String id; // TODO: use UUID type
        @NonNull String path;
        @NonNull long used;
        @NonNull long total;
    }

    @Data
    @RequiredArgsConstructor
    @NoArgsConstructor
    public static class ReplicaSet {
        final List<Disk> disks = new ArrayList<>();
    }
}
