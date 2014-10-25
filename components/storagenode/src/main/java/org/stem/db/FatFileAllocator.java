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

import com.google.common.io.Closer;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stem.util.BBUtils;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

public class FatFileAllocator {
    private static final Logger logger = LoggerFactory.getLogger(MountPoint.class);

    private static final String FAT_FILE_NAME_TEMPLATE = "stem-ff-%06d.db";
    public static final String FAT_FILE_NAME_REGEX = "stem-ff-([0-9]{6}).db";

    public static void allocateFile(String filePath, long sizeInMB) throws IOException {
        allocateFile(filePath, sizeInMB, false);
    }

    public static FatFile create(String filePath, long sizeInMB) throws IOException {
        allocateFile(filePath, sizeInMB, true);
        return FatFile.open(filePath, new DataTracker(StorageNodeDescriptor.getCluster().getvBucketsNum()));
    }

    public static void allocateFile(String filePath, long sizeInMB, boolean mark) throws IOException {
        long started = System.currentTimeMillis();

        Closer closer = Closer.create();
        try {
            File file = new File(filePath);
            if (file.exists())
                throw new IOException(String.format("File already exists: %s", filePath));

            RandomAccessFile rw = closer.register(new RandomAccessFile(file, "rw"));
            rw.setLength(sizeInMB * 1024 * 1024);
            if (mark) {
                rw.seek(0);
                rw.writeByte(FatFile.MARKER_BLANK);
                rw.seek(rw.length() - 1);
                rw.writeByte(FatFile.MARKER_BLANK);
            }
        }
        catch (Throwable e) {
            throw closer.rethrow(e);
        }
        finally {
            closer.close();
            logger.debug("{} was allocated in {} ms", filePath, System.currentTimeMillis() - started);
        }
    }


    public static void allocateDirectory(String directoryPath, long sizeInMB, long maxSize, boolean mark) throws IOException {
        long started = System.currentTimeMillis();
        File dir = BBUtils.getDirectory(directoryPath);
        // TODO: empty check

        long allocated = sizeInMB * 1024 * 1024;
        int fatFileIndex = 0;

        //while (FileSystemUtils.freeSpaceKb(directoryPath)/1024 > sizeInMB)
        while (maxSize >= allocated) {
            String fatFilePath = dir.getAbsolutePath() + File.separator + buildFileName(fatFileIndex);
            allocateFile(fatFilePath, sizeInMB, mark);
            allocated += sizeInMB * 1024 * 1024;
            fatFileIndex++;
        }
        int totalAllocated = fatFileIndex;
        if (0 == totalAllocated)
            throw new RuntimeException("Can't preallocate, maybe there is no free space on the device " + directoryPath);

        logger.debug("Directory {} ({}, {} files) was allocated in {} ms",
                directoryPath,
                FileUtils.byteCountToDisplaySize(maxSize),
                fatFileIndex,
                System.currentTimeMillis() - started);


    }

    /**
     * Allocate a directory with files of a given size
     *
     * @param directoryPath
     * @param sizeInMB
     * @throws IOException
     */
    public static void allocateDirectory(String directoryPath, long sizeInMB) throws IOException {
        File dir = BBUtils.getDirectory(directoryPath);
        allocateDirectory(directoryPath, sizeInMB, dir.getFreeSpace(), false);
    }

    public static String buildFileName(int fatFileIndex) {
        return String.format(FAT_FILE_NAME_TEMPLATE, fatFileIndex);
    }

}
