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

import java.util.ArrayList;
import java.util.List;

public class EventSubscriber implements TopologyEventListener {

    private List<TopologyEventListener> listeners = new ArrayList<>();

    public EventSubscriber(Topology topology) {
    }

    public EventSubscriber() {
    }

    public void addListener(TopologyEventListener listener) {
        listeners.add(listener);
    }

    public void removeListener(TopologyEventListener listener) {
        listeners.add(listener);
    }


    @Override
    public void onDatacenterAdded(Topology.Datacenter dc) {
        for (TopologyEventListener listener : listeners) {
            listener.onDatacenterAdded(dc);
        }
    }

    @Override
    public void onDatacenterRemoved(Topology.Datacenter dc) {
        for (TopologyEventListener listener : listeners) {
            listener.onDatacenterRemoved(dc);
        }
    }

    @Override
    public void onRackAdded(Topology.Rack rack) {
        for (TopologyEventListener listener : listeners) {
            listener.onRackAdded(rack);
        }
    }

    @Override
    public void onRackRemoved(Topology.Rack rack) {
        for (TopologyEventListener listener : listeners) {
            listener.onRackRemoved(rack);
        }
    }

    @Override
    public void onStorageNodeAdded(Topology.StorageNode node) {
        for (TopologyEventListener listener : listeners) {
            listener.onStorageNodeAdded(node);
        }
    }

    @Override
    public void onStorageNodeRemoved(Topology.StorageNode node) {
        for (TopologyEventListener listener : listeners) {
            listener.onStorageNodeRemoved(node);
        }
    }

    @Override
    public void onDiskAdded(Topology.Disk disk) {
        for (TopologyEventListener listener : listeners) {
            listener.onDiskAdded(disk);
        }
    }

    @Override
    public void onDiskRemoved(Topology.Disk disk) {
        for (TopologyEventListener listener : listeners) {
            listener.onDiskRemoved(disk);
        }
    }
}
