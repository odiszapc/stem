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

package org.stem.streaming;

import com.twitter.crunch.MappingDiff;
import com.twitter.crunch.Node;
import org.stem.utils.TopologyUtils;

import java.util.*;

public class DiskMovementMap {

    /**
     * {
     * "src_disk_UUID": {
     *         vBucket: "dst_disk_UUID"
     *     }
     * }
     */
    private Map<String, Map<Long, String>> diskMovementMap = new HashMap<String, Map<Long, String>>();

    private DiskMovementMap(Map<String, Map<Long, String>> diskMovementMap, boolean b) // "b" to avoid erasure ambiguity
    {
        this.diskMovementMap = diskMovementMap;
    }

    public Map<String, Map<Long, String>> getMap() {
        return diskMovementMap;
    }

    public DiskMovementMap(Map<Long, List<MappingDiff.Value<Node>>> mappingDiff) // TODO: translate to factory method
    {
        for (Map.Entry<Long, List<MappingDiff.Value<Node>>> entry : mappingDiff.entrySet()) {
            Long vBucket = entry.getKey();
            List<MappingDiff.Value<Node>> diffs = entry.getValue();

            List<String> disksOut = new ArrayList<String>(); // disks we are going stream from
            List<String> disksIn = new ArrayList<String>(); // the endpoints of streams

            for (MappingDiff.Value<Node> diff : diffs) {
                String diskId = TopologyUtils.extractDiskUUID(diff.get().getName());

                switch (diff.getDifferenceType()) {
                    case ADDED:
                        disksIn.add(diskId);
                        break;
                    case REMOVED:
                        disksOut.add(diskId);
                        break;
                }
            }

            // TODO: !!!! This MUST be investigated with love (case: disks of the different sizes) !!!
            if (disksIn.size() != disksOut.size())
                throw new RuntimeException("Number of disks removed is not equal to added ones");

            for (int i = 0; i < disksOut.size(); i++) {
                String out = disksOut.get(i);
                String in = disksIn.get(i);

                Map<Long, String> movementInfo = diskMovementMap.get(out);
                if (null == movementInfo) {
                    movementInfo = new HashMap<Long, String>();
                    diskMovementMap.put(out, movementInfo);
                }
                movementInfo.put(vBucket, in);
            }
        }
    }

    public int getDisksInvolved() {
        return diskMovementMap.size();
    }

    public int size() {
        return diskMovementMap.size();
    }

    public long getVBucketsNumber(UUID diskId) {
        Map<Long, String> buckets = diskMovementMap.get(diskId.toString());
        if (null == buckets)
            return 0;

        return buckets.size();
    }

    public int getReceiversNumber(UUID diskId) {
        Map<Long, String> buckets = diskMovementMap.get(diskId.toString());
        if (null == buckets)
            return 0;

        Set<String> receivers = new HashSet<String>(buckets.values());
        return receivers.size();
    }

    public DiskMovementMap getSlice(List<UUID> disksIds) {
        Set<UUID> ids = new HashSet<UUID>(disksIds);

        Map<String, Map<Long, String>> slice = new HashMap<String, Map<Long, String>>();
        for (Map.Entry<String, Map<Long, String>> entry : diskMovementMap.entrySet()) {
            String srcDisk = entry.getKey();
            Map<Long, String> dst = entry.getValue();

            if (ids.contains(UUID.fromString(srcDisk))) {
                slice.put(srcDisk, dst);
            }
        }

        return new DiskMovementMap(slice, true);
    }
}


































