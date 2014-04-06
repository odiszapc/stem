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

import org.stem.domain.ArrayBalancer;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

public class DataTracker
{
    ConcurrentMap<Long, AtomicLong> totalCountMap = new ConcurrentHashMap<Long, AtomicLong>();
    ConcurrentMap<Long, AtomicLong> liveCountMap = new ConcurrentHashMap<Long, AtomicLong>();
    ConcurrentMap<Long, AtomicLong> totalSizeMap = new ConcurrentHashMap<Long, AtomicLong>();
    ConcurrentMap<Long, AtomicLong> liveSizeMap = new ConcurrentHashMap<Long, AtomicLong>();
    ConcurrentMap<Integer, AtomicLong> deletesPerFFCountMap = new ConcurrentHashMap<Integer, AtomicLong>();
    ConcurrentMap<Integer, AtomicLong> deletesPerFFSizeMap = new ConcurrentHashMap<Integer, AtomicLong>();

    // Total number of blobs
    AtomicLong totalCount = new AtomicLong(0);

    // Number of non-deleted blobs
    AtomicLong liveCount = new AtomicLong(0);

    // Number of FULL fat files
    AtomicLong fullFatFileCount = new AtomicLong(0);

    // number of BLANK fat files
    AtomicLong blankFatFileCount = new AtomicLong(0);

    AtomicLong fatFileCount = new AtomicLong(0);

    // Total size of all blobs stored in (deleted + live)
    AtomicLong totalSizeInBytes = new AtomicLong(0);

    // Size of non-deleted blobs
    AtomicLong liveSizeInBytes = new AtomicLong(0);

    // Total size of full fat files
    AtomicLong fullFatFileSizeInBytes = new AtomicLong(0);

    AtomicLong allocatedSizeInBytes = new AtomicLong(0);

    AtomicLong deletesCount = new AtomicLong(0);

    AtomicLong deletesSizeInBytes = new AtomicLong(0);

    private ArrayBalancer dht; // TODO: share instance by making it static

    public DataTracker(int vBuckets)
    {
        dht = new ArrayBalancer(vBuckets);
    }

    public void count(FatFileIndex index)
    {
        for (FatFileIndex.Entry entry : index.getEntries())
        {
            count(entry);
        }
    }

    public void count(FatFileIndex.Entry entry)
    {
        count(entry.key, entry.length, entry.isLive());
    }

    public void count(Blob.Header header)
    {
        count(header.key, header.length, header.isLive());
    }

    public long getTotalBlobs()
    {
        return totalCount.get();
    }

    public long getLiveBlobs()
    {
        return liveCount.get();
    }

    public long getTotalSizeInBytes()
    {
        return totalSizeInBytes.get();
    }

    public long getLiveSizeInBytes()
    {
        return liveSizeInBytes.get();
    }

    public long getAllocatedSizeInBytes()
    {
        return allocatedSizeInBytes.get();
    }

    public long getDeletesSizeInBytes()
    {
        return deletesSizeInBytes.get();
    }

    public long getDeletesSizeInBytes(int ff)
    {
        AtomicLong ref = deletesPerFFSizeMap.get(ff);
        if (null == ref)
            return 0;

        return ref.get();
    }

    public long getDeletesCount()
    {
        return deletesCount.get();
    }

    public long getDeletesCount(int ff)
    {
        AtomicLong ref = deletesPerFFCountMap.get(ff);
        if (null == ref)
            return 0;

        return ref.get();
    }

    public Long getVBucket(byte[] key)
    {
        return dht.getToken(key).longValue();
    }

    public void count(byte[] keyBytes, long size, boolean live)
    {
        Long vBucket = dht.getToken(keyBytes).longValue();
        initCounters(vBucket);

        totalCountMap.get(vBucket).incrementAndGet();
        totalSizeMap.get(vBucket).addAndGet(size);

        totalCount.incrementAndGet();
        totalSizeInBytes.addAndGet(size);

        if (live)
        {
            liveCountMap.get(vBucket).incrementAndGet();
            liveSizeMap.get(vBucket).addAndGet(size);

            liveCount.incrementAndGet();
            liveSizeInBytes.addAndGet(size);
        }
    }

    private void initCounters(long vBucket)
    {
        totalCountMap.putIfAbsent(vBucket, new AtomicLong(0));
        totalSizeMap.putIfAbsent(vBucket, new AtomicLong(0));
        liveCountMap.putIfAbsent(vBucket, new AtomicLong(0));
        liveSizeMap.putIfAbsent(vBucket, new AtomicLong(0));
    }

    public void discount(byte[] keyBytes, long size, int ff)
    {
        Long vBucket = dht.getToken(keyBytes).longValue();
        initCounters(vBucket);

        liveCountMap.get(vBucket).decrementAndGet();
        liveSizeMap.get(vBucket).addAndGet(-size);

        liveCount.decrementAndGet();
        liveSizeInBytes.addAndGet(-size);

        deletesPerFFCountMap.putIfAbsent(ff, new AtomicLong(0));
        deletesPerFFSizeMap.putIfAbsent(ff, new AtomicLong(0));
        deletesPerFFCountMap.get(ff).incrementAndGet();
        deletesPerFFSizeMap.get(ff).addAndGet(size);

        deletesCount.incrementAndGet();
        deletesSizeInBytes.addAndGet(size);
    }

    public void remove(byte[] keyBytes, long size)
    {
        Long vBucket = dht.getToken(keyBytes).longValue();
        initCounters(vBucket);

        totalCountMap.get(vBucket).decrementAndGet();
        totalSizeMap.get(vBucket).addAndGet(-size);

        totalCount.decrementAndGet();
        totalSizeInBytes.addAndGet(-size);

        liveCountMap.get(vBucket).decrementAndGet();
        liveSizeMap.get(vBucket).addAndGet(-size);

        liveCount.decrementAndGet();
        liveSizeInBytes.addAndGet(-size);
    }

    public void removeDeletes(byte[] keyBytes, long size, int ff)
    {
        Long vBucket = dht.getToken(keyBytes).longValue();
        initCounters(vBucket);

        deletesPerFFCountMap.putIfAbsent(ff, new AtomicLong(0));
        deletesPerFFSizeMap.putIfAbsent(ff, new AtomicLong(0));
        deletesPerFFCountMap.get(ff).decrementAndGet();
        deletesPerFFSizeMap.get(ff).addAndGet(-size);

        deletesCount.decrementAndGet();
        deletesSizeInBytes.addAndGet(-size);
    }

    public void incFullFatFiles(long size)
    {
        fullFatFileCount.incrementAndGet();
        fullFatFileSizeInBytes.addAndGet(size);

        fatFileCount.incrementAndGet();
        allocatedSizeInBytes.addAndGet(size);
    }

    public void incBlankFatFiles(long size)
    {
        fatFileCount.incrementAndGet();
        blankFatFileCount.incrementAndGet();
        allocatedSizeInBytes.addAndGet(size);
    }

    public void turnIntoBlank(long size)
    {
        fullFatFileCount.decrementAndGet();
        fullFatFileSizeInBytes.addAndGet(-size);
        blankFatFileCount.incrementAndGet();
    }

    public void turnIntoFull(long size)
    {
        fullFatFileCount.incrementAndGet();
        fullFatFileSizeInBytes.addAndGet(size);
        blankFatFileCount.decrementAndGet();
    }

    public void turnIntoActive()
    {
        blankFatFileCount.decrementAndGet();
    }

    public void setActiveFatFile(long size)
    {
        fatFileCount.incrementAndGet();
        allocatedSizeInBytes.addAndGet(size);
    }

    public long getBlankFatFileCount()
    {
        return blankFatFileCount.get();
    }

    public long getFullFatFileCount()
    {
        return fullFatFileCount.get();
    }

    public long getFatFileCount()
    {
        return fatFileCount.get();
    }

    public long getLiveBucketSize(long vBucket)
    {
        AtomicLong atomic = liveSizeMap.get(vBucket);
        if (null == atomic)
            return 0;
        return atomic.get();
    }
}
