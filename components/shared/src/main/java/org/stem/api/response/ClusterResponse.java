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

public class ClusterResponse extends StemResponse {
    Cluster cluster = new Cluster();

    public Cluster getCluster() {
        return cluster;
    }

    public static class Cluster {
        String name;
        int vBucketsNum;
        int rf;
        long usedBytes;
        long totalBytes;

        List<Storage> nodes = new ArrayList<Storage>();

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getvBucketsNum() {
            return vBucketsNum;
        }

        public void setvBucketsNum(int vBucketsNum) {
            this.vBucketsNum = vBucketsNum;
        }

        public int getRf() {
            return rf;
        }

        public void setRf(int rf) {
            this.rf = rf;
        }

        public long getUsedBytes() {
            return usedBytes;
        }

        public void setUsedBytes(long usedBytes) {
            this.usedBytes = usedBytes;
        }

        public long getTotalBytes() {
            return totalBytes;
        }

        public void setTotalBytes(long totalBytes) {
            this.totalBytes = totalBytes;
        }

        public List<Storage> getNodes() {
            return nodes;
        }

        public void setNodes(List<Storage> nodes) {
            this.nodes = nodes;
        }

        @Override
        public String toString() {
            return "Cluster{" +
                    "name='" + name + '\'' +
                    ", vBucketsNum=" + vBucketsNum +
                    ", rf=" + rf +
                    ", usedBytes=" + usedBytes +
                    ", totalBytes=" + totalBytes +
                    ", nodes=" + nodes +
                    '}';
        }
    }

    public static class Storage {
        String endpoint;
        long usedBytes;
        long totalBytes;

        List<Disk> disks = new ArrayList<Disk>();

        public Storage() {
        }

        public Storage(String ip, int port, long used, long total) {
            endpoint = ip + ":" + port;
            usedBytes = used;
            totalBytes = total;
        }

        public String getEndpoint() {
            return endpoint;
        }

        public void setEndpoint(String endpoint) {
            this.endpoint = endpoint;
        }

        public long getUsedBytes() {
            return usedBytes;
        }

        public void setUsedBytes(long usedBytes) {
            this.usedBytes = usedBytes;
        }

        public long getTotalBytes() {
            return totalBytes;
        }

        public void setTotalBytes(long totalBytes) {
            this.totalBytes = totalBytes;
        }

        public List<Disk> getDisks() {
            return disks;
        }

        public void setDisks(List<Disk> disks) {
            this.disks = disks;
        }
    }

    public static class Disk {
        String id;
        String path;
        long usedBytes;
        long totalBytes;

        public Disk() {
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getPath() {
            return path;
        }

        public void setPath(String path) {
            this.path = path;
        }

        public long getUsedBytes() {
            return usedBytes;
        }

        public void setUsedBytes(long usedBytes) {
            this.usedBytes = usedBytes;
        }

        public long getTotalBytes() {
            return totalBytes;
        }

        public void setTotalBytes(long totalBytes) {
            this.totalBytes = totalBytes;
        }
    }
}
