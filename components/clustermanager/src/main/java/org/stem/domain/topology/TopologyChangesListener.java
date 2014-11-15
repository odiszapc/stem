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

public abstract class TopologyChangesListener implements TopologyEventListener {

    public abstract void onTopologyUpdated(Topology.Node node);

    @Override
    public void onDatacenterAdded(Topology.Datacenter dc) {
        onTopologyUpdated(dc);
    }

    @Override
    public void onDatacenterRemoved(Topology.Datacenter dc) {
        onTopologyUpdated(dc);
    }

    @Override
    public void onDiskAdded(Topology.Disk disk) {
        onTopologyUpdated(disk);
    }

    @Override
    public void onDiskRemoved(Topology.Disk disk) {
        onTopologyUpdated(disk);
    }

    @Override
    public void onRackAdded(Topology.Rack rack) {
        onTopologyUpdated(rack);
    }

    @Override
    public void onRackRemoved(Topology.Rack rack) {
        onTopologyUpdated(rack);
    }

    @Override
    public void onStorageNodeAdded(Topology.StorageNode node) {
        onTopologyUpdated(node);
    }

    @Override
    public void onStorageNodeRemoved(Topology.StorageNode node) {
        onTopologyUpdated(node);
    }
}
