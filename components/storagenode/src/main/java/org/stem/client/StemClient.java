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

package org.stem.client;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import org.stem.coordination.TopoMapping;
import org.stem.coordination.ZooConstants;
import org.stem.coordination.ZookeeperClient;
import org.stem.coordination.ZookeeperClientFactory;
import org.stem.domain.ArrayBalancer;
import org.stem.domain.ExtendedBlobDescriptor;
import org.stem.transport.Message;
import org.stem.transport.ops.DeleteBlobMessage;
import org.stem.transport.ops.ResultMessage;
import org.stem.transport.ops.WriteBlobMessage;

import java.util.*;

public class StemClient implements TopoMapSubscriber {

    private MetaStoreClient metaClient;
    private ZookeeperClient zooClient;
    private TopoMapListener mappingListener; // Updated from Zookeeper size by ClusterManager
    private TopoMapping mapping;
    private ArrayBalancer dht;

    StorageNodeClient nodeClient = new StorageNodeClient("localhost:9999");

    public StemClient() {
        dht = new ArrayBalancer(1);
        mappingListener = new TopoMapListener();
        mappingListener.listen(this);
        metaClient = new MetaStoreClient();
        zooClient = ZookeeperClientFactory.newClient();
    }

    public void start() {
        try {
            metaClient.start();
            zooClient.start();
            // TODO: get mappings directly
            mapping = zooClient.readZNodeData(ZooConstants.TOPOLOGY + "/" + ZooConstants.TOPO_MAP, TopoMapping.class);
            mappingChanged(mapping);

            zooClient.listenForZNode(ZooConstants.TOPOLOGY + "/" + ZooConstants.TOPO_MAP, mappingListener);
        } catch (Exception e) {
            throw new RuntimeException("Can't start Stem client", e);
        }
    }

    public TopoMapping getMapping() {
        return mapping;
    }

    private List<UUID> getDisks(Integer vBucket) {
        return mapping.getDisks(vBucket.longValue());
    }

    private String getStorageNodeEntpoint(UUID diskId) {
        return mapping.getStorageNodeEntpoint(diskId);
    }

    @Override
    public void mappingChanged(TopoMapping newMapping) {
        mapping = newMapping;

        if (dht.size() != newMapping.CRUSHMapSize()) {
            dht = new ArrayBalancer(mapping.CRUSHMapSize());
        }
    }

    public void put(byte[] key, byte[] blob) {
        assert 16 == key.length;
        // TODO: max-size of blob check

        Integer vBucket = dht.getToken(key);
        List<UUID> disks = getDisks(vBucket); // TODO: no disk ?

        put(key, blob, disks);
    }

    public void put(byte[] key, byte[] blob, UUID disk) {
        put(key, blob, Lists.newArrayList(disk));
    }

    public void put(byte[] key, byte[] blob, List<UUID> disks) {
        assert 16 == key.length;
        // TODO: max-size of blob check

        //Map<String, ExtendedBlobDescriptor> success = new HashMap<String, ExtendedBlobDescriptor>();
        // success.put("localhost:9999", new ExtendedBlobDescriptor(key, blob.length, disks.get(0), 0, 0, 0)); // STUB
        Map<String, ExtendedBlobDescriptor> success = writeToStorageNodes(key, blob, disks);


        if (success.isEmpty())
            throw new RuntimeException("All writes to storage nodes failed, RF = " + disks.size());

        metaClient.writeMeta(success.values());
    }

    private Map<String, ExtendedBlobDescriptor> writeToStorageNodes(byte[] key, byte[] blob, List<UUID> disks) {
        Map<String, RequestFuture> futures = new HashMap<String, RequestFuture>();
        Map<String, WriteBlobMessage> requestsMap = new HashMap<String, WriteBlobMessage>();
        for (UUID diskId : disks) {
            String endpoint = getStorageNodeEntpoint(diskId);
            StorageNodeClient nodeClient = new StorageNodeClient(endpoint); // TODO: this is valid but very slow to create instance each time

            WriteBlobMessage request = new WriteBlobMessage(diskId, key, blob); // TODO: pass member fields to constructor
            requestsMap.put(endpoint, request);
            RequestFuture future = nodeClient.executeAsync(request, true);
            futures.put(endpoint, future);
        }

        // Wait for results
        Map<String, Message.Response> writeResults = new HashMap<String, Message.Response>();
        for (Map.Entry<String, RequestFuture> entry : futures.entrySet()) {
            String endpoint = entry.getKey();
            RequestFuture future = entry.getValue();

            Message.Response result = future.getUninterruptibly();
            writeResults.put(endpoint, result);
        }

        // Collect results
        Map<String, ExtendedBlobDescriptor> success = new HashMap<String, ExtendedBlobDescriptor>(); // There is something like List<ExtendedBlobDescriptor>
        for (Map.Entry<String, Message.Response> entry : writeResults.entrySet()) {
            String endpoint = entry.getKey();
            Message.Response resp = entry.getValue();
            if (resp.isSuccess()) {
                ResultMessage.WriteBlob writeResp = (ResultMessage.WriteBlob) resp;
                ExtendedBlobDescriptor result = new ExtendedBlobDescriptor(
                        requestsMap.get(endpoint).key,
                        requestsMap.get(endpoint).getBlobSize(),
                        requestsMap.get(endpoint).disk,
                        writeResp.getFatFileIndex(),
                        -1,
                        writeResp.getOffset()); // TODO: remove -1 part
                success.put(endpoint, result);
            }
        }

        return success;
    }

    public byte[] get(byte[] key) {
        List<ExtendedBlobDescriptor> metaResults = metaClient.readMeta(key);
        if (0 == metaResults.size())
            return null;
        //throw new RuntimeException("No meta information about key: 0x" + Hex.encodeHexString(key));

        Random random = new Random();
        int index = random.nextInt(metaResults.size());
        ExtendedBlobDescriptor selectedMeta = metaResults.get(index);

        String endpoint = getStorageNodeEntpoint(selectedMeta.getDisk()); // TODO: null check
        StorageNodeClient nodeClient = new StorageNodeClient(endpoint); // TODO: avoid to create client each time, pool should be used

        byte[] data = nodeClient.readBlob(
                selectedMeta.getDisk(),
                selectedMeta.getFFIndex(),
                selectedMeta.getBodyOffset(),
                selectedMeta.getLength());

        // TODO: retries an other nodes if no data found

        return data;
    }

    @VisibleForTesting
    public String getFirstEndpointForKey(byte[] key) {
        List<ExtendedBlobDescriptor> metaResults = metaClient.readMeta(key);
        ExtendedBlobDescriptor selectedMeta = metaResults.get(0);
        return getStorageNodeEntpoint(selectedMeta.getDisk());
    }

    @VisibleForTesting
    public ExtendedBlobDescriptor getFirstDescriptorForKey(byte[] key) {
        List<ExtendedBlobDescriptor> metaResults = metaClient.readMeta(key);
        return metaResults.get(0);
    }

    public void delete(byte[] key) {
        assert 16 == key.length;
        // TODO: max-size of blob check

        // Delete from Meta store
        List<ExtendedBlobDescriptor> metaResults = metaClient.readMeta(key);

        // TODO: if no meta?

        metaClient.deleteMeta(key);
        Map<String, ResultMessage.Void> responses = deleteFromStorageNodes(metaResults);
    }

    private Map<String, ResultMessage.Void> deleteFromStorageNodes(List<ExtendedBlobDescriptor> metaResults) {
        Map<String, RequestFuture> futures = new HashMap<String, RequestFuture>();
        Map<String, DeleteBlobMessage> requestsMap = new HashMap<String, DeleteBlobMessage>();
        for (ExtendedBlobDescriptor meta : metaResults) {
            String endpoint = getStorageNodeEntpoint(meta.getDisk());
            StorageNodeClient nodeClient = new StorageNodeClient(endpoint); // TODO: this is valid but very slow to create instance each time

            DeleteBlobMessage request = new DeleteBlobMessage(meta.getDisk(), meta.getFFIndex(), meta.getBodyOffset());
            requestsMap.put(endpoint, request);
            RequestFuture future = nodeClient.executeAsync(request, true);
            futures.put(endpoint, future);
        }

        // Wait for results
        Map<String, Message.Response> writeResults = new HashMap<String, Message.Response>();
        for (Map.Entry<String, RequestFuture> entry : futures.entrySet()) {
            String endpoint = entry.getKey();
            RequestFuture future = entry.getValue();

            Message.Response result = future.getUninterruptibly();
            writeResults.put(endpoint, result);
        }

        // Collect results
        Map<String, ResultMessage.Void> success = new HashMap<String, ResultMessage.Void>(); // There is something like List<ExtendedBlobDescriptor>
        for (Map.Entry<String, Message.Response> entry : writeResults.entrySet()) {
            String endpoint = entry.getKey();
            Message.Response resp = entry.getValue();
            if (resp.isSuccess()) {
                ResultMessage.Void writeResp = (ResultMessage.Void) resp;
                success.put(endpoint, writeResp);
            }
        }

        return success;
    }

    public void removeReplica(byte[] key, UUID diskId) {
        metaClient.deleteReplica(key, diskId);
    }
}
