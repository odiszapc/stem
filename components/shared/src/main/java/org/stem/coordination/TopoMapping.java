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

package org.stem.coordination;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class TopoMapping extends ZNodeAbstract {

    Map<Long, List<UUID>> bucketMap = new HashMap<Long, List<UUID>>();
    Map<UUID, String> diskMap = new HashMap<UUID, String>();

    public Map<Long, List<UUID>> getBucketMap() {
        return bucketMap;
    }

    public Map<UUID, String> getDiskMap() {
        return diskMap;
    }

    public int CRUSHMapSize() {
        return bucketMap.size();
    }

    @Override
    public String name() {
        return ZooConstants.TOPO_MAP;
    }

    public TopoMapping() {
    }

    public TopoMapping(Map<Long, List<UUID>> bucketMap, Map<UUID, String> diskMap) {
        this.bucketMap = bucketMap;
        this.diskMap = diskMap;
    }

    public List<UUID> getDisks(Long vBucket) {
        return bucketMap.get(vBucket);
    }

    public String getStorageNodeEntpoint(UUID diskId) {
        return diskMap.get(diskId);
    }

}
