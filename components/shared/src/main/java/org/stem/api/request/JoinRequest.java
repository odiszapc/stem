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

package org.stem.api.request;


import java.util.ArrayList;
import java.util.List;

public class JoinRequest extends ClusterManagerRequest {
    private int port;
    private List<String> ipAddresses = new ArrayList<String>();
    private String host;
    private List<Disk> disks = new ArrayList<Disk>();

    public List<String> getIpAddresses() {
        return ipAddresses;
    }

    public List<Disk> getDisks() {
        return disks;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public JoinRequest() {
    }

    public JoinRequest(String host, int port, List<String> ipAddresses, List<Disk> disks) {
        this.host = host;
        this.port = port;
        this.ipAddresses = ipAddresses;
        this.disks = disks;
    }

    public static class Disk {
        private String id;
        private String path;
        private long totalSizeInBytes;
        private long usedSizeInBytes;

        public String getId() {
            return id;
        }

        public String getPath() {
            return path;
        }

        public long getTotalSizeInBytes() {
            return totalSizeInBytes;
        }

        public long getUsedSizeInBytes() {
            return usedSizeInBytes;
        }

        public Disk() {
        }

        public Disk(String id, String path, long usedSizeInBytes, long totalSizeInBytes) {
            this.id = id;
            this.path = path;
            this.usedSizeInBytes = usedSizeInBytes;
            this.totalSizeInBytes = totalSizeInBytes;
        }
    }
}
