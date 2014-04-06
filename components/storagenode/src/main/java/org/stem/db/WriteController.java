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

package org.stem.db;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stem.domain.BlobDescriptor;
import org.stem.transport.ops.WriteBlobMessage;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class WriteController extends IOController
{
    private static final Logger logger = LoggerFactory.getLogger(WriteController.class);
    private boolean pureAllocated = false;

    private ThreadPoolExecutor executor;
    private CandidatesProvider candidatesProvider;
    FatFile activeFF;

    @VisibleForTesting
    public int getWriteCandidates()
    {
        return candidatesProvider.candidates.size();
    }

    public WriteController(MountPoint mp)
    {
        super(mp);

        executor = new ThreadPoolExecutor(1, 1, 60, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
        candidatesProvider = new CandidatesProvider(mp.findBlankOrActive());
        activeFF = nextFile();

        if (activeFF.isBlank())
        {
            this.pureAllocated = true;
        }

        try
        {
            activeFF.seek(activeFF.getPointer());
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    private static int writes = 0;

    public synchronized BlobDescriptor write(WriteBlobMessage message)
    {
        try
        {
            if (!activeFF.hasSpaceFor(message.getBlobSize()))
            {
                long freeSpace = activeFF.getFreeSpace();
                activeFF.writeIndex();
                activeFF.writeFullMarker();
                int idPrev = activeFF.id;
                activeFF = nextFile();
                int idNext = activeFF.id;

                // Compute counters
                mp.getDataTracker().turnIntoFull(StorageNodeDescriptor.getFatFileSizeInMb() * 1024 * 1024);
                logger.info(String.format("%s: FatFile #%s is full (%,d bytes free), switched to #%s", this.mp.path, idPrev, freeSpace, idNext));

                return write(message);
            }

            if (0 == activeFF.getPointer())
            {
                activeFF.writeActiveMarker();
                if (pureAllocated)
                {
                    pureAllocated = false;
                    mp.getDataTracker().turnIntoActive();
                }
            }

            return activeFF.writeBlob(message.key, message.blob);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    private FatFile nextFile()
    {
        return candidatesProvider.provide();
    }

    public void submitBlankFF(FatFile ff)
    {
        candidatesProvider.put(ff);
    }

    public static class CandidatesProvider
    {
        private Queue<FatFile> candidates;

        public CandidatesProvider(Collection<FatFile> list)
        {
            candidates = new LinkedList<FatFile>();
            candidates.addAll(list);
        }

        public synchronized FatFile provide() // TODO: synchronized is bad
        {
            FatFile candidate = candidates.poll();
            if (null == candidate)
                throw new RuntimeException("No candidates");
            return candidate;
        }

        public synchronized void add(FatFile candidate)
        {
            candidates.add(candidate);
        }

        public synchronized void put(FatFile ff)
        {
            candidates.add(ff);
        }
    }
}
