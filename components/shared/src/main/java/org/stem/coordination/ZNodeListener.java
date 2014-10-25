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

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.curator.utils.ZKPaths;

public class ZNodeListener implements PathChildrenCacheListener, NodeCacheListener {
    private ZNodeEventHandler handler;
    private NodeCache nodeCache;

    public ZNodeListener(ZNodeEventHandler handler) {
        this.handler = handler;
    }

    public ZNodeListener(ZNodeEventHandler handler, NodeCache nodeCache) {
        this.handler = handler;
        this.nodeCache = nodeCache;
    }

    @Override
    public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
        switch (event.getType()) {
            case CHILD_ADDED: {
                handler.onChildAdded(ZKPaths.getNodeFromPath(event.getData().getPath()), event.getData().getData(), event.getData().getStat());
                break;
            }

            case CHILD_UPDATED: {
                handler.onChildUpdated(ZKPaths.getNodeFromPath(event.getData().getPath()), event.getData().getData(), event.getData().getStat());
                break;
            }

            case CHILD_REMOVED: {
                handler.onChildRemoved(ZKPaths.getNodeFromPath(event.getData().getPath()), event.getData().getData(), event.getData().getStat());
                break;
            }
        }
    }

    @Override
    public void nodeChanged() throws Exception {
        if (null == handler) {
            // TODO: warn
            return;
        }

        ChildData snapshot = nodeCache.getCurrentData();
        handler.onNodeUpdated(
                snapshot.getPath(),
                snapshot.getData(),
                snapshot.getStat());
    }
}
