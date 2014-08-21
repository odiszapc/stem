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
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.Predicate;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stem.util.BBUtils;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.*;

public class MountPoint
{
    private static final Logger logger = LoggerFactory.getLogger(MountPoint.class);
    public static final String ID_FILENAME = "id";

    public UUID uuid;
    public String path;
    public long totalSpace;
    public long usedSpace;

    private Map<Integer, FatFile> fatFiles = new TreeMap<Integer, FatFile>(); // TODO: initial capacity

    private DataTracker dataTracker;

    public DataTracker getDataTracker()
    {
        return dataTracker;
    }

    private MountPoint(String path, long totalSpace, DataTracker dataTracker)
    {
        this.path = path;
        this.totalSpace = totalSpace;
        this.dataTracker = dataTracker;
    }

    @VisibleForTesting
    public MountPoint(UUID uuid, String path, long totalSpace, DataTracker dataTracker)
    {
        this.uuid = uuid;
        this.path = path;
        this.totalSpace = totalSpace;
        this.dataTracker = dataTracker;
    }

    public static MountPoint open(String path, DataTracker dataTracker) throws IOException
    {
        return open(path, dataTracker, true);
    }

    public static MountPoint open(String directoryPath, DataTracker dataTracker, boolean init) throws IOException
    {
        File dir = BBUtils.getDirectory(directoryPath);
        long capacity = dir.getFreeSpace(); // For small fat files

        MountPoint mountPoint = new MountPoint(directoryPath, capacity, dataTracker);
        if (init)
        {
            mountPoint.init();
        }
        return mountPoint;
    }

    public FatFile getFatFile(int index)
    {
        return fatFiles.get(index);
    }

    private void init() throws IOException
    {
        File dir = BBUtils.getDirectory(path);
        File[] allFiles = dir.listFiles();
        File[] fatFiles = listFatFiles();
        uuid = readIdent();

        if (0 != fatFiles.length)
        {
            if (null == uuid)
                throw new IOException("Ident file not found for mount point " + path);
        } else
        {
            assert null != allFiles;
            if (0 != allFiles.length)
                throw new IOException("Mount point " + path + " is not empty");

            uuid = generateIdent();
            writeIdent();


            long maxAllocation = 0 != StorageNodeDescriptor.getMaxAllocationInMb()
                    ? StorageNodeDescriptor.getMaxAllocationInMb() * 1024 * 1024
                    : totalSpace;

            if (StorageNodeDescriptor.getAutoAllocate())
            {
                FatFileAllocator.allocateDirectory(
                        path,
                        StorageNodeDescriptor.getFatFileSizeInMb(),
                        maxAllocation,
                        StorageNodeDescriptor.getMarkOnAllocate());
            } // TODO: what should we do in auto-allocation is turned off?
        }

        // TODO: open descriptors
        // TODO: check fat file size with pre-configured
        fatFiles = listFatFiles();
        for (File fatFile : fatFiles)
        {
            logger.info("Opening {}", fatFile.getAbsolutePath());
            FatFile file = FatFile.open(fatFile.getAbsolutePath(), dataTracker);
            this.fatFiles.put(file.id, file);
        }

        // keep in mind ACTIVE fat files during computing used space
        this.usedSpace = findBlankOrActive().size() * StorageNodeDescriptor.getFatFileSizeInMb();
    }

    private void writeIdent() throws IOException
    {
        assert null != uuid;
        String idFilePath = path + File.separator + ID_FILENAME;
        FileUtils.writeStringToFile(new File(idFilePath), uuid.toString());
    }

    private UUID generateIdent()
    {
        return UUID.randomUUID();
    }

    private File[] listFatFiles() throws IOException
    {
        File dir = BBUtils.getDirectory(path);
        return dir.listFiles(new FilenameFilter()
        {
            @Override
            public boolean accept(File dir, String name)
            {
                return name.matches(FatFileAllocator.FAT_FILE_NAME_REGEX);
            }
        });
    }

    private UUID readIdent() throws IOException
    {
        String idFilePath = path + File.separator + ID_FILENAME;
        File file = new File(idFilePath);
        if (!file.exists())
            return null;

        String uuidString = FileUtils.readFileToString(file);
        return UUID.fromString(uuidString);
    }

    public void close()
    {
        for (FatFile fatFile : fatFiles.values())
        {
            fatFile.close();
        }
    }

    public Collection<FatFile> findBlankOrActive()
    {
        Collection<FatFile> collection = CollectionUtils.select(fatFiles.values(), new Predicate<FatFile>()
        {
            @Override
            public boolean evaluate(FatFile file)
            {
                return file.isBlank() || file.isActive();
            }
        });

        List<FatFile> sortedList = new ArrayList<FatFile>(collection);

        // Sort, ACTIVE files is placed first
        Collections.sort(sortedList, new Comparator<FatFile>()
        {
            @Override
            public int compare(FatFile f1, FatFile f2)
            {
                if (f1.getState() == f2.getState())
                    return f1.id.compareTo(f2.id);

                else
                {
                    Integer state1 = f1.getState().ordinal();
                    Integer state2 = f2.getState().ordinal();
                    return state1.compareTo(state2);
                }
            }
        });

        return sortedList;
    }

    public Collection<FatFile> findFullOrActive()
    {
        Collection<FatFile> collection = CollectionUtils.select(fatFiles.values(), new Predicate<FatFile>()
        {
            @Override
            public boolean evaluate(FatFile file)
            {
                return file.isFull() || file.isActive();
            }
        });

        List<FatFile> sortedList = new ArrayList<FatFile>(collection);

        // Sort, ACTIVE files is placed first
        Collections.sort(sortedList, new Comparator<FatFile>()
        {
            @Override
            public int compare(FatFile f1, FatFile f2)
            {
                return f1.id.compareTo(f2.id);
            }
        });

        return sortedList;
    }

    public Collection<FatFile> findFull()
    {
        Collection<FatFile> collection = CollectionUtils.select(fatFiles.values(), new Predicate<FatFile>()
        {
            @Override
            public boolean evaluate(FatFile file)
            {
                return file.isFull();
            }
        });

        List<FatFile> sortedList = new ArrayList<FatFile>(collection);

        Collections.sort(sortedList, new Comparator<FatFile>()
        {
            @Override
            public int compare(FatFile f1, FatFile f2)
            {
                if (f1.getState() == f2.getState())
                    return f1.id.compareTo(f2.id);

                else
                {
                    Integer state1 = f1.getState().ordinal();
                    Integer state2 = f2.getState().ordinal();
                    return state1.compareTo(state2);
                }
            }
        });

        return sortedList;
    }

    public Collection<FatFile> findReadyForCompaction()
    {
        Collection<FatFile> full = findFull();

        return CollectionUtils.select(full, new Predicate<FatFile>()
        {
            @Override
            public boolean evaluate(FatFile file)
            {
                return null != dataTracker.deletesPerFFCountMap.get(file.id);
            }
        });
    }

    public long getTotalSizeInBytes()
    {
        return dataTracker.getTotalSizeInBytes();
    }

    public long getLiveSizeInBytes()
    {
        return dataTracker.getLiveSizeInBytes();
    }

    public long getAllocatedSizeInBytes()
    {
        return dataTracker.getAllocatedSizeInBytes();
    }

    public String getPath()
    {
        return path;
    }
}
