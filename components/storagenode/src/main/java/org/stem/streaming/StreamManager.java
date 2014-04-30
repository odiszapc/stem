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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stem.coordination.ZooConstants;
import org.stem.coordination.StemZooEventHandler;
import org.stem.coordination.ZookeeperClient;
import org.stem.coordination.ZookeeperClientFactory;
import org.stem.db.DataTracker;
import org.stem.db.Layout;
import org.stem.db.StorageNodeDescriptor;
import org.stem.util.Utils;

import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class StreamManager
{
    private static final Logger logger = LoggerFactory.getLogger(StreamManager.class);
    public static final StreamManager instance = new StreamManager();
    private Executor pool = Executors.newFixedThreadPool(1);

    private ZookeeperClient client;

    public StreamManager()
    {
        client = ZookeeperClientFactory.create();
        client.start();
    }

    public void listenForSessions()
    {
        try
        {
            client.listenForChildren(ZooConstants.OUT_SESSIONS, new NewSessionsListener());
        }
        catch (Exception e)
        {
            throw new RuntimeException("Can't start listen for sessions");
        }
    }

    public void startStreaming(StreamSession session)
    {
        // TODO: unique, concurrency validations
        // TODO: don't stream if nothing to stream

        // First let's calculate size of data we need to stream
        for (DiskMovement movTask : session.getMovements().values())
        {
            UUID disk = movTask.getDiskId();
            DataTracker tracker = Layout.getInstance().getMountPoint(disk).getDataTracker();
            long dataTransferSizeInBytes = 0;
            for (BucketStreamingPart part : movTask.getBucketStreams().values())
            {
                long size = tracker.getLiveBucketSize(part.getvBucket());
                dataTransferSizeInBytes += size;
            }

            movTask.init(session, 0, dataTransferSizeInBytes);

            StreamTask streamTask = new StreamTask(movTask);
            pool.execute(streamTask); // TODO: stats action?
        }
    }

    private class NewSessionsListener extends StemZooEventHandler<StreamSession>
    {
        @Override
        public Class<? extends StreamSession> getBaseClass()
        {
            return StreamSession.class;
        }

        @Override
        protected void onChildAdded(StreamSession session)
        {
            // TODO: if it's my session
            String host = Utils.getHostFromEndpoint(session.endpoint);
            int port = Utils.getPortFromEndpoint(session.endpoint);
            if (!(
                    StorageNodeDescriptor.getNodeListen().equals(host) &&
                            StorageNodeDescriptor.getNodePort() == port))
            {
                return;
            }

            session.setProgress(0);
            session.setTotal(0);

            for (DiskMovement movTask : session.getMovements().values())
            {
                movTask.setProgress(0);
                movTask.setTotal(0);
            }

            startStreaming(session);
        }
    }
}
