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
import org.stem.transport.ops.DeleteBlobMessage;
import org.stem.transport.ops.ReadBlobMessage;
import org.stem.transport.ops.WriteBlobMessage;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class StorageService
{
    private static final Logger logger = LoggerFactory.getLogger(StorageService.class);

    public static final StorageService instance = new StorageService();

    private final Map<UUID, WriteController> wControllers;
    private final Map<UUID, ReadController> rControllers;

    @VisibleForTesting
    public int getWriteCandidates(UUID disk)
    {
        return wControllers.get(disk).getWriteCandidates();
    }

    public BlobDescriptor write(WriteBlobMessage message)
    {
        try
        {
            WriteController wc = wControllers.get(message.disk);
            if (null == wc)
                throw new RuntimeException(String.format("Mount point %s can not be found", message.disk));

            return wc.write(message);
        }
        catch (Exception e)
        {
            logger.error("Error writing blob", e);
            throw new RuntimeException(e);
        }
    }

    public byte[] read(ReadBlobMessage message)
    {
        ReadController controller = rControllers.get(message.disk); // TODO: if not found?
        return controller.read(message.fatFileIndex, message.offset, message.length);
    }

    public void delete(DeleteBlobMessage message)
    {
        ReadController controller = rControllers.get(message.disk); // TODO: find by message directly
        controller.delete(message.fatFileIndex, message.offset);
    }

    public StorageService()
    {
        wControllers = new HashMap<UUID, WriteController>(Layout.getInstance().getMountPoints().size());
        rControllers = new HashMap<UUID, ReadController>(Layout.getInstance().getMountPoints().size());
    }

    public void submitFF(FatFile ff, MountPoint mp)
    {
        assert ff.isBlank(); // TODO: normal check with Exception throw
        WriteController controller = wControllers.get(mp.uuid);
        if (null == controller)
            throw new RuntimeException("shit happens"); // TODO: shit is bad

        controller.submitBlankFF(ff);
    }

    public void init()
    {
        for (MountPoint mp : Layout.getInstance().getMountPoints().values())
        {
            WriteController wc = new WriteController(mp);
            ReadController rc = new ReadController(mp);
            wControllers.put(mp.uuid, wc);
            rControllers.put(mp.uuid, rc);
        }
    }

}
