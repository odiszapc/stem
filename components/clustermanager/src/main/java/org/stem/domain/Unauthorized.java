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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stem.coordination.Event;
import org.stem.coordination.EventFuture;
import org.stem.coordination.EventManager;
import org.stem.coordination.ZNodeAbstract;
import org.stem.domain.topology.Topology;
import org.stem.exceptions.StemException;

import java.util.*;

public class Unauthorized {

    private static final Logger logger = LoggerFactory.getLogger(Unauthorized.class);

    private final Cluster cluster;
    final Map<UUID, NodeInsertMeta> registry = new HashMap<>();

    public Unauthorized(Cluster cluster) {
        this.cluster = cluster;
    }

    public void add(Topology.StorageNode node, EventFuture future) {
        // TODO: validation StorageNode object
        synchronized (cluster) {
            if (null != registry.get(node.id))
                throw new StemException(String.format("Node with id=%s is already in pending list", node.id));

            Topology.StorageNode existing = cluster.topology2.findStorageNode(node.id);
            if (null != existing)
                throw new StemException(String.format("Node with id=%s already exist in cluster", node.id));

            NodeInsertMeta meta = new NodeInsertMeta(node, future);
            registry.put(node.id, meta);
        }
    }

    //TODO: clear registry or delete specific node

    public List<Topology.StorageNode> list() {

        List<Topology.StorageNode> result = new ArrayList<>();
        for (NodeInsertMeta meta : registry.values()) {
            result.add(meta.node);
        }
        return result;
    }

    public Topology.StorageNode get(UUID id) {
        return getMeta(id).node;
    }

    private NodeInsertMeta getMeta(UUID id) {
        NodeInsertMeta meta = registry.get(id);
        if (null == meta)
            throw new StemException(String.format("Node with id=%s can not be found", id));
        else
            return meta;
    }

    public Event.Join approveNew(UUID id, String datacenter, String rack) {
        synchronized (cluster) {
            NodeInsertMeta meta = getMeta(id);
            EventFuture future = meta.future();
            Event.Join result;
            //try {
            cluster.addStorageNode(meta.node, datacenter, rack);
            registry.remove(id);
            result = success("Node was successfully added to cluster");
            future.setResult(result);
            return result;

//            } catch (TopologyException e) {
//                result = failed("Node can not be added to cluster: " + e.getMessage());
//                future.setResult(result);
//                logger.error("Failed to add node to cluster", e);
//                return result;
//
//            } catch (Exception e) {
//                result = failed("Node can not be added to cluster: Unexpected error");
//                future.setResult(result);
//                logger.error("Failed to add node to cluster", e);
//                return result;
//            }
        }
    }

    public Event.Join approveExisting(UUID uuid) {
        try {
            synchronized (cluster) {
                EventFuture future = EventManager.instance.readSubscription(uuid);
                Event.Join result = success("Existing node has successfully joined back to cluster");
                future.setResult(result);
                return result;
            }
        } catch (Exception e) {
            return failed("Node can not be added to cluster: Unexpected error");
            // TODO
        }
    }

    public Event.Join deny(UUID id) {
        synchronized (cluster) {
            NodeInsertMeta meta = getMeta(id);
            EventFuture future = meta.future();
            Event.Join result;

            registry.remove(id);
            result = failed("User refuses node with id=" + id);
            future.setResult(result);
            return result;
        }
    }

    private static Event.Join success(String message) {
        return new Event.Join(Event.Join.Result.SUCCESS, message);
    }

    private static Event.Join failed(String message) {
        return new Event.Join(Event.Join.Result.ERROR, message);
    }

    /**
     *
     */
    private class NodeInsertMeta extends ZNodeAbstract {

        private Topology.StorageNode node;
        private UUID eventId;
        private EventFuture future;

        public NodeInsertMeta(Topology.StorageNode node, EventFuture future) {
            this.node = node;
            this.future = future;
            this.eventId = future.eventId();
        }

        public NodeInsertMeta(Topology.StorageNode node, UUID eventId) throws Exception {
            this.node = node;
            this.future = EventManager.instance.readSubscription(eventId);
            this.eventId = eventId;
        }

        public NodeInsertMeta() {
        }

        public EventFuture future() {
            return future;
        }

        @Override
        public String name() {
            return node.getId().toString();
        }
    }
}
