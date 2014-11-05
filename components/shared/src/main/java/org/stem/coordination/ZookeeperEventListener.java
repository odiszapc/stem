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

import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stem.utils.JsonUtils;

public abstract class ZookeeperEventListener<T> {

    private static final Logger logger = LoggerFactory.getLogger(ZookeeperEventListener.class);

    private ZNodeEventHandler handler;

    public ZNodeEventHandler getHandler() {
        return handler;
    }

    protected ZookeeperEventListener() {
        handler = new ZNodeEventHandler() {
            @Override
            public void onChildAdded(String path, byte[] data, Stat stat) {
                try {
                    ZookeeperEventListener.this.onChildAdded(decode(data));
                } catch (Exception e) {
                    onError(e);
                }
            }

            @Override
            public void onChildUpdated(String path, byte[] data, Stat stat) {
                try {
                    ZookeeperEventListener.this.onChildUpdated(decode(data));
                } catch (Exception e) {
                    onError(e);
                }
            }

            @Override
            public void onChildRemoved(String path, byte[] data, Stat stat) {
                try {
                    ZookeeperEventListener.this.onChildRemoved(decode(data));
                } catch (Exception e) {
                    onError(e);
                }
            }

            @Override
            public void onNodeUpdated(String path, byte[] data, Stat stat) {
                try {
                    ZookeeperEventListener.this.onNodeUpdated(decode(data));
                } catch (Exception e) {
                    onError(e);
                }
            }
        };
    }

    private T decode(byte[] data) {
        return JsonUtils.decode(data, getBaseClass());
    }

    protected void onChildAdded(T object) {
    }

    protected void onChildUpdated(T object) {
    }

    protected void onChildRemoved(T object) {
    }

    protected void onNodeUpdated(T object) {
    }

    protected void onError(Throwable t) {
        // TODO: log or throw?
        logger.error("Error while processing event from Zookeeper", t);
    }

    public abstract Class<? extends T> getBaseClass();
}
