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

import org.stem.api.request.JoinRequest;
import org.stem.coordination.DiskStat;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class StorageNode
{
    String ipAddress;
    int port;
    List<Disk> disks;

    public String getIpAddress()
    {
        return ipAddress;
    }

    public void setIpAddress(String ipAddress)
    {
        this.ipAddress = ipAddress;
    }

    public int getPort()
    {
        return port;
    }

    public void setPort(int port)
    {
        this.port = port;
    }

    public List<Disk> getDisks()
    {
        return disks;
    }

    public void setDisks(List<DiskStat> disks)
    {
        List<Disk> newDisks = new ArrayList<Disk>(disks.size());
        for (DiskStat stat : disks)
        {
            Disk disk = new Disk(stat.getId());
            disk.setPath(stat.getPath());
            disk.setTotalBytes(stat.getTotalBytes());
            disk.setUsedBytes(stat.getUsedBytes());
            newDisks.add(disk);
        }
        this.disks = newDisks;
    }

    public String getEndpoint()
    {
        return ipAddress + ":" + port;
    }

    public StorageNode(String ipAddress, int port, List<JoinRequest.Disk> disksFromRequest)
    {
        this.ipAddress = ipAddress;
        this.port = port;
        this.disks = new ArrayList<Disk>();
        for (JoinRequest.Disk diskREST : disksFromRequest)
        {
            Disk disk = new Disk(diskREST.getId());
            disk.setPath(diskREST.getPath());
            disk.setUsedBytes(diskREST.getUsedSizeInBytes());
            disk.setTotalBytes(diskREST.getTotalSizeInBytes());

            disks.add(disk);
        }
    }

    public StorageNode(JoinRequest req)
    {
        this(req.getHost(), req.getPort(), req.getDisks());
    }

    public List<String> getDiskIds()
    {
        List<String> ids = new ArrayList<String>();
        for (Disk disk : disks)
        {
            ids.add(disk.getId());
        }
        return ids;
    }

    public List<UUID> getDiskUUIDs()
    {
        List<UUID> ids = new ArrayList<UUID>();
        for (Disk disk : disks)
        {
            ids.add(UUID.fromString(disk.getId()));
        }
        return ids;
    }

    public long getUsedBytes()
    {
        long sum = 0;
        for (Disk disk : disks)
        {
            sum += disk.getUsedBytes();
        }
        return sum;
    }

    public long getTotalBytes()
    {
        long sum = 0;
        for (Disk disk : disks)
        {
            sum += disk.getTotalBytes();
        }
        return sum;
    }

    public StorageNode()
    {
    }
}
