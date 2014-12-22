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

import org.stem.domain.topology.DataMapping;
import org.stem.domain.topology.Partitioner;
import org.stem.domain.topology.Topology;
import org.stem.exceptions.TopologyException;
import org.stem.utils.TopologyUtils;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class DataDistributionManager {

    final Partitioner partitioner;

    private final Topology topology;
    private Cluster cluster;
    private ArrayBalancer keysDistributor;

    private final AtomicReference<DataMapping> current = new AtomicReference<>(new DataMapping());
    private final AtomicReference<DataMapping> previous = new AtomicReference<>(new DataMapping());

    public DataDistributionManager(Cluster cluster, Partitioner partitioner) {
        this.cluster = cluster;
        this.partitioner = partitioner;
        this.topology = cluster.topology();
    }

    public DataDistributionManager(Cluster cluster, Partitioner partitioner,
                                   DataMapping currentMapping, DataMapping previousMapping) {
        this(cluster, partitioner);
        this.current.set(currentMapping);
        this.previous.set(previousMapping);
    }

    public DataMapping getCurrentMappings() {
        return current.get();
    }

    public DataMapping getPreviousMapping() {
        return previous.get();
    }

    public synchronized DataMapping computeMappingNonMutable() {
        DataMapping current = computeDataMapping(cluster.descriptor().rf, cluster.descriptor().vBuckets, topology);
        DataMapping previous = this.current.getAndSet(current);
        this.previous.set(previous);
        return current;
    }

    public DataMapping.Difference computeMappingDifference() {
        DataMapping current = this.current.get();
        DataMapping previous = this.previous.get();

        if (current.isEmpty() && previous.isEmpty())
            throw new TopologyException("Can not calculate difference between empty data mappings");

        if (current.isEmpty()) {
            current = previous;
        } else if (null == previous) {
            previous = current;
        }

        return computeMappingDifference(previous, current);
    }

    public DataMapping.Difference computeMappingDifference(DataMapping before, DataMapping after) {
        return DataMapping.Difference.compute(before, after);
    }

    private DataMapping computeDataMapping(int rf, int buckets, org.stem.domain.topology.Topology topology) {
        List<Long> longs = TopologyUtils.prepareBucketsArray(buckets);

        Map<Long, org.stem.domain.topology.Topology.ReplicaSet> map = (Map<Long, org.stem.domain.topology.Topology.ReplicaSet>) partitioner.algorithm().computeMapping(longs, rf, topology);
        return new DataMapping(map);
    }
}
