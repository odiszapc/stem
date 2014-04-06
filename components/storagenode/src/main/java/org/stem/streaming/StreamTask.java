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

import org.apache.commons.codec.binary.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stem.client.StemClient;
import org.stem.client.StorageNodeClient;
import org.stem.db.*;
import org.stem.db.compaction.FFScanner;
import org.stem.transport.ops.DeleteBlobMessage;
import org.stem.util.WrappedRunnable;

import java.io.IOException;
import java.util.Collection;
import java.util.UUID;


public class StreamTask extends WrappedRunnable
{
    private static final Logger logger = LoggerFactory.getLogger(StreamTask.class);
    private final DiskMovement movement;
    private final MountPoint disk;
    private final DataTracker datatracker;
    private final StemClient clusterClient;

    public StreamTask(DiskMovement movement)
    {
        // TODO: null check
        this.movement = movement;
        this.disk = Layout.instance.getMountPoint(movement.getDiskId());
        this.datatracker = this.disk.getDataTracker();

        this.clusterClient = new StemClient();
    }

    @Override
    protected void runMayThrow() throws Exception
    {
        // TODO: validate progress, total
        // TODO: resume functionality
        // TODO: what if task has failed but it's possible to recover? (network occasions, other bullshit)

        long started = System.currentTimeMillis();
        start();
        long duration = System.currentTimeMillis() - started;
    }

    private void start() throws IOException
    {
        clusterClient.start();
        Collection<FatFile> fatFiles = disk.findFullOrActive();

        int moved = 0;
        for (FatFile ff : fatFiles)
        {
            FFScanner scanner = new FFScanner(ff);
            while (scanner.hasNext())
            {
                Blob blob = scanner.next();
                long bucket = datatracker.getVBucket(blob.key());
                BucketStreamingPart part = movement.get(bucket);
                if (null != part)
                {
                    moveBlob(blob, disk.uuid, part.getEndpoint(), part.getDiskId());
                    moved++;
                }
            }
        }

        logger.info("Streaming has been completed, blobs streamed: {}", moved);
    }

    private void moveBlob(Blob blob, UUID localDiskId, String remoteEndpoint, UUID remoteDiskId) throws IOException
    {
        StorageNodeClient client = new StorageNodeClient(remoteEndpoint); // TODO: create pool of clients to reuse them
        client.start();

        //WriteBlobMessage message = new WriteBlobMessage(remoteDiskId, blob.key(), blob.data());
        //BlobDescriptor response = client.writeBlob(message);

        clusterClient.put(blob.key(), blob.data(), remoteDiskId); // add a new replica reference (this don't delete old one)

        clusterClient.removeReplica(blob.key(), localDiskId); // remove the old replica reference
        DeleteBlobMessage delete = new DeleteBlobMessage(
                localDiskId,
                blob.getDescriptor().getFFIndex(),
                blob.getDescriptor().getBodyOffset());
        StorageService.instance.delete(delete);

        logger.info("key 0x{} was streamed to {}", Hex.encodeHexString(blob.key()), remoteEndpoint);

        movement.progress(blob.size());
        int a = 1;
    }
}
