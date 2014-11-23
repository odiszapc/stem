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


import lombok.Data;
import org.stem.api.REST;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ClusterResponse extends StemResponse { // TODO: Rest.ClusterDescriptor

    Cluster cluster = new Cluster();

    public Cluster getCluster() {
        return cluster;
    }

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

        List<REST.StorageNode> nodes = new ArrayList<REST.StorageNode>();

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
                    ", partitioner='" + partitioner + '\'' +
                    ", zookeeperEndpoint='" + zookeeperEndpoint + '\'' +
                    ", metaStoreContactPoints=" + Arrays.toString(metaStoreContactPoints) +
                    '}';
        }
    }
}
