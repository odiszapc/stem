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

import com.twitter.crunch.*;
import org.stem.streaming.BucketStreamingPart;
import org.stem.streaming.DiskMovement;
import org.stem.streaming.DiskMovementMap;
import org.stem.streaming.StreamSession;
import org.stem.util.TopologyUtils;

import java.util.*;

public class Topology {
    private Node crunchTopology;
    private Map<Long, List<Node>> CRUSHMap;
    private Map<Long, List<MappingDiff.Value<Node>>> mappingDiff = null;

    private SimpleCRUSHMapping mappingFunction;

    private Map<UUID, StorageNode> disksMap = new HashMap<UUID, StorageNode>();

    private Map<String, StorageNode> storagesMap = new HashMap<String, StorageNode>();

    public Collection<StorageNode> getStorages() {
        return storagesMap.values();
    }

    public Map<Long, List<Node>> getCRUSHMap() {
        return CRUSHMap;
    }

    public Map<UUID, StorageNode> getDisksMap() {
        return disksMap;
    }

    public Topology(String name, int rf) {
        crunchTopology = TopologyUtils.createSingleDCCluster(name);
        PlacementRules rules = new RackIsolationPlacementRules();
        mappingFunction = new SimpleCRUSHMapping(rf, rules);
    }

    public boolean storageExists(StorageNode storageNode) {
        return null != storagesMap.get(storageNode.getEndpoint());
    }

    public boolean storageExists(String endpoint) {
        return null != storagesMap.get(endpoint);
    }

    // TODO: What should we do when storage going down?
    public void addStorage(StorageNode storageNode) {
        Node storage = TopologyUtils.createStorage(storageNode.getEndpoint(), storageNode.getDiskIds());
        Node rack = TopologyUtils.createSingleStorageRack(storage);
        TopologyUtils.addChildren(rack, getDC());

        for (String diskId : storageNode.getDiskIds()) {
            disksMap.put(UUID.fromString(diskId), storageNode);
        }
        storagesMap.put(storageNode.getEndpoint(), storageNode);
    }

    private Node getDC() {
        return crunchTopology.findChildren(StorageSystemTypes.DATA_CENTER).get(0);
    }

    public void computeMappings(int vBuckets) {
        List<Long> vBucketsIds = TopologyUtils.generateVBucketsIds(vBuckets);
        Map<Long, List<Node>> newMapping = mappingFunction.computeMapping(vBucketsIds, crunchTopology);

        if (null != this.CRUSHMap) {
            mappingDiff = MappingDiff.calculateDiff(CRUSHMap, newMapping);
        }

        CRUSHMap = newMapping;
    }

    public List<StreamSession> computeStreamingSessions()   // TODO: synchronized ?
    {
        List<StreamSession> sessions = new ArrayList<StreamSession>();

        if (null == mappingDiff)
            return sessions;

        DiskMovementMap diskMovementMap = new DiskMovementMap(mappingDiff);
        // Generate session

        for (StorageNode outStorageNode : getStorages()) {
            DiskMovementMap slice = diskMovementMap.getSlice(outStorageNode.getDiskUUIDs());
            if (0 == slice.size()) {
                continue;
            }

            Map<UUID, DiskMovement> movements = new HashMap<UUID, DiskMovement>();
            for (Map.Entry<String, Map<Long, String>> entry : slice.getMap().entrySet()) {
                //Map<Long, BucketStreamingPart> inBucketParts = new HashMap<Long, BucketStreamingPart>();
                UUID outDisk = UUID.fromString(entry.getKey());
                DiskMovement diskMovement = new DiskMovement(outDisk);
                Map<Long, String> in = entry.getValue();
                for (Map.Entry<Long, String> entry2 : in.entrySet()) {
                    Long vBucket = entry2.getKey();
                    UUID inDisk = UUID.fromString(entry2.getValue()); // TODO: null check
                    StorageNode inStorageNode = disksMap.get(inDisk); // TODO: null check

                    BucketStreamingPart part = new BucketStreamingPart(vBucket, inStorageNode.getEndpoint(), inDisk);
                    diskMovement.put(vBucket, part);
                }
                movements.put(outDisk, diskMovement);
            }

            StreamSession outgoingSession = new StreamSession(outStorageNode.getEndpoint(), movements);
            sessions.add(outgoingSession);
        }
        return sessions;
    }

    public StorageNode getStorage(String endpoint) {
        return storagesMap.get(endpoint);
    }
}

