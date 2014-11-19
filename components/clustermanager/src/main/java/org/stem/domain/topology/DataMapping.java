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

package org.stem.domain.topology;

import com.google.common.collect.Lists;

import java.util.*;

public class DataMapping {

    public static final DataMapping EMPTY = new DataMapping();

    private final Map<Long, Topology.ReplicaSet> map = new HashMap<>();

    public Map<Long, Topology.ReplicaSet> getMap() {
        return map;
    }

    public List<Long> getBuckets() {
        return Lists.newArrayList(map.keySet());
    }

    public Topology.ReplicaSet getReplicas(Long bucket) {
        return map.get(bucket);
    }

    /**
     * Class that calculates how disk were moved between arbitrary data mappings
     * Thanks to developers of libcrunch (Twitter)
     * This class is a port of com.twitter.crunch.MappingDiff
     */
    public static class Difference {

        public static Difference compute(DataMapping before, DataMapping after) {
            return new Difference(before, after);
        }

        Map<Long, List<Delta>> result = new HashMap<>();

        protected Difference(DataMapping before, DataMapping after) {
            for (Long key : before.getBuckets()) {
                Topology.ReplicaSet l1 = before.getReplicas(key);
                Topology.ReplicaSet l2 = after.getReplicas(key);
                List<Delta> diff = calculateDiff(l1, l2);
                if (!diff.isEmpty())
                    result.put(key, diff);
            }

            List<Long> m2Keys = after.getBuckets();
            m2Keys.removeAll(before.getBuckets());
            for (Long key : m2Keys) {
                Topology.ReplicaSet list = after.getReplicas(key);
                if (!list.isEmpty())
                    result.put(key, wrapList(list, Move.ADDED));
            }
        }

        private static List<Delta> calculateDiff(Topology.ReplicaSet before, Topology.ReplicaSet after) {
            if (null == before && null == after)
                return Collections.emptyList();

            if (null == before)
                return wrapList(after, Move.ADDED);

            if (null == after)
                return wrapList(before, Move.REMOVED);

            List<Delta> result = new ArrayList<>();
            for (Topology.Disk disk : before) {
                if (!after.contains(disk))
                    result.add(new Delta(disk, Move.REMOVED));
            }

            for (Topology.Disk disk : after) {
                if (!before.contains(disk))
                    result.add(new Delta(disk, Move.ADDED));
            }
            return result;
        }

        private static List<Delta> wrapList(Topology.ReplicaSet list, Move diff) {
            List<Delta> result = new ArrayList<>();
            for (Topology.Disk disk : list) {
                result.add(new Delta(disk, diff));
            }
            return result;
        }

        public static class Delta {

            public final Topology.Disk value;
            public final Move move;

            public Delta(Topology.Disk value, Move move) {
                this.value = value;
                this.move = move;
            }
        }

        public enum Move {ADDED, REMOVED}
    }
}
