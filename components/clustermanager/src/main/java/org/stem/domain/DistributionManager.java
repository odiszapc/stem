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

package org.stem.domain;

import org.stem.domain.topology.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class DistributionManager {

    final Partitioner partitioner;
    private Cluster cluster;
    private ArrayBalancer keysDistributor;

    public DistributionManager(Partitioner partitioner, Cluster cluster) {
        this.partitioner = partitioner;
        this.cluster = cluster;
    }

    public synchronized void computeMapping() {
        computeDataMap(cluster.descriptor().rf, cluster.descriptor().vBuckets, cluster.topology());
    }

    private void computeDataMap(int rf, int buckets, org.stem.domain.topology.Topology topology) {
        List<Long> longs = prepareBucketsArray(buckets);

        Map map = (Map<Long, org.stem.domain.topology.Topology.ReplicaSet>) partitioner.algorithm().computeMapping(longs, rf, topology);
    }

    private static List<Long> prepareBucketsArray(int buckets) {
        Long[] arr = new Long[buckets];
        for (int i = 0; i < buckets; i++) {
            arr[i] = (long) i;
        }

        return Arrays.asList(arr);
    }
}
