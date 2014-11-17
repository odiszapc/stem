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


import org.stem.api.REST;

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
        String partitioner;
        String zookeeperEndpoint;
        long usedBytes;
        long totalBytes;

        List<REST.StorageNode> nodes = new ArrayList<REST.StorageNode>();

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

        public String getPartitioner() {
            return partitioner;
        }

        public void setPartitioner(String partitioner) {
            this.partitioner = partitioner;
        }

        public String getZookeeperEndpoint() {
            return zookeeperEndpoint;
        }

        public void setZookeeperEndpoint(String zookeeperEndpoint) {
            this.zookeeperEndpoint = zookeeperEndpoint;
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

        public List<REST.StorageNode> getNodes() {
            return nodes;
        }

        public void setNodes(List<REST.StorageNode> nodes) {
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
}
