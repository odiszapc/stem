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
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.data.Stat;
import org.stem.util.JsonUtils;

public class ZookeeperClient
{
    CuratorFramework client;

    // TODO: Zookeeper endpoint should be configurable
    public ZookeeperClient()
    {
        client = CuratorFrameworkFactory.newClient("localhost:2181", new ExponentialBackoffRetry(1000, 3));
    }

    public void start()
    {
        client.start();
    }

    public void listenChildren(String path, ZNodeEventHandler handler) throws Exception
    {
        PathChildrenCache cache = new PathChildrenCache(client, path, true);
        cache.start();

        PathChildrenCacheListener listener = new ZNodeListener(handler);
        cache.getListenable().addListener(listener);
    }

    /**
     * Listen for a single node
     *
     * @param path
     * @param listener
     * @throws Exception
     */
    public void listenForZNode(String path, StemZooEventHandler listener) throws Exception
    {
        NodeCache cache = new NodeCache(client, path);
        cache.start();

        NodeCacheListener cacheListener = new ZNodeListener(
                listener.getHandler(), cache);

        cache.getListenable().addListener(cacheListener);
    }

    public void listenForChildren(String path, StemZooEventHandler listener) throws Exception
    {
        PathChildrenCache cache = new PathChildrenCache(client, path, true);
        cache.start();

        PathChildrenCacheListener cacheListener = new ZNodeListener(
                listener.getHandler());

        cache.getListenable().addListener(cacheListener);
    }

    public void createIfNotExists(String path) throws Exception
    {
        try
        {
            Stat stat = client.checkExists().forPath(path);
            if (null == stat)
            {
                client.create().creatingParentsIfNeeded().forPath(path);
            }
        }
        catch (Exception e)
        {
            throw new Exception("Error occurred during interaction with Zookeeper", e);
        }
    }

    public void createNodeIfNotExists(String parent, ZNode znode) throws Exception
    {
        if (!nodeExists(parent, znode))
            createNode(parent, znode);
    }

    public boolean nodeExists(String parent, ZNode znode) throws Exception
    {
        String path = ZKPaths.makePath(parent, znode.nodeName());
        return null != client.checkExists().forPath(path);
    }

    public void createNode(String parent, ZNode znode) throws Exception // TODO: if already exists?
    {
        String path = ZKPaths.makePath(parent, znode.nodeName());
        createNode(path, znode.encode());
    }

    public void updateNode(String parent, ZNode znode) throws Exception // TODO: automatically add/remove trailing slash
    {
        String path = ZKPaths.makePath(parent, znode.nodeName());
        updateNode(path, znode.encode());
    }

    public <T extends ZNode> T readZNode(String path, Class<T> clazz) throws Exception
    {
        byte[] data = client.getData().forPath(path);
        return JsonUtils.decode(data, clazz);
    }

    public void createNode(String path, byte[] data) throws Exception
    {
        client.create().creatingParentsIfNeeded().forPath(path, data);
    }

    public void updateNode(String path, byte[] data) throws Exception
    {
        client.setData().forPath(path, data);
    }

    public void removeNode(String path) throws Exception
    {
        client.delete().forPath(path);
    }
}
