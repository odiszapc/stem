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

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.stem.utils.Utils;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public abstract class REST {

    @Data
    @RequiredArgsConstructor
    public static class Topology {

        final List<Datacenter> dataCenters = new ArrayList<>();
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

    @Data
    @RequiredArgsConstructor
    @NoArgsConstructor
    public static class StorageNode {

        @NonNull UUID id;
        @NonNull String hostname;
        @NonNull String listen;
        @NonNull Long capacity;

        final List<Disk> disks = new ArrayList<>();
        final List<String> ipAddresses = new ArrayList<String>();

        public String getListenHost() {
            return Utils.getHost(listen);
        }

        public int getListenPort() {
            return Utils.getPort(listen);
        }

        public void setListen(String host, int port) {
            this.listen = host + ':' + port;
        }
    }

    @Data
    @RequiredArgsConstructor
    @NoArgsConstructor
    public static class Disk {

        @NonNull String id; // TODO: use UUID type
        @NonNull String path;
        @NonNull long total;
        @NonNull long used;
    }
}
