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

package org.stem;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class TestUtil
{
    public static final String TMP_DATA_DIR_NAME = "tmp-io";
    public static final String TMP_DATA_DIR;

    static {
        String resourceRoot = TestUtil.class.getResource("/").getPath();
        TMP_DATA_DIR = resourceRoot + TMP_DATA_DIR_NAME;
    }

    public static String temporize(String path)
    {
        return TMP_DATA_DIR + File.separator + path;
    }

    public static void createTempDir() throws IOException
    {
        File dir = new File(TMP_DATA_DIR);
        if (!(dir.exists() && dir.isDirectory()))
            createDir(TMP_DATA_DIR);
    }

    public static void cleanupTempDir() throws IOException
    {
        File dir = new File(TMP_DATA_DIR);
        delete(dir);
    }

    public static void emptyDir(String path) throws IOException
    {
        File dir = new File(path);
        assert dir.isDirectory();
        for (File file : dir.listFiles())
        {
            delete(file);
        }
    }

    public static String getDirInTmp(String path)
    {
        String temporized = temporize(path);
        File dir = new File(temporized);
        if (!dir.exists())
            createDir(temporized);
        return temporized;
    }

    public static String createDirInTmp(String path)
    {
        String temporized = temporize(path);
        createDir(path);
        return temporized;
    }

    public static void createDir(String path)
    {
        File dir = new File(path);
        if (!dir.mkdirs())
        {
            throw new RuntimeException("Can not create directory: " + path);
        }
    }

    public static void delete(String path) throws IOException
    {
        delete(new File(path));
    }

    public static void delete(File file) throws IOException
    {
        if (!file.exists())
            return;

        if (file.isDirectory())
        {
            if (file.list().length == 0)
            {
                file.delete();
            } else
            {
                String files[] = file.list();

                for (String temp : files)
                {
                    File fileDelete = new File(file, temp);
                    delete(fileDelete);
                }

                //check the directory again, if empty then delete it
                if (file.list().length == 0)
                {
                    file.delete();
                }
            }
        } else
        {
            file.delete();
        }
    }

    public static List<byte[]> generateRandomBlobs(int from, int to)
    {
        int chunksCount = from + (int) (Math.random() * ((to - from) + 1));
        List<byte[]> chunks = new ArrayList<byte[]>(chunksCount);

        Random staticRand = new Random(System.currentTimeMillis());

        for (int i = 0; i < chunksCount; i++)
        {
            int size = (i == chunksCount - 1) ? staticRand.nextInt(65536 / 2) : 65536;
            byte[] chunk = new byte[size];
            for (int k = 0; k < chunk.length; k++)
            {
                chunk[i] = (byte) (Math.random() * 255);
            }
            chunks.add(chunk);
        }
        return chunks;
    }

    public static List<byte[]> generateRandomBlobs(int chunksCount)
    {
        List<byte[]> chunks = new ArrayList<byte[]>(chunksCount);

        Random staticRand = new Random(System.currentTimeMillis());

        for (int i = 0; i < chunksCount; i++)
        {
            int size = (i == chunksCount - 1) ? staticRand.nextInt(65536 / 2) : 65536;
            byte[] chunk = new byte[size];
            for (int k = 0; k < chunk.length; k++)
            {
                chunk[i] = (byte) (Math.random() * 255);
            }
            chunks.add(chunk);
        }
        return chunks;
    }

    public static byte[] generateRandomBlob()
    {
        return generateRandomBlob(65536);
    }

    public static byte[] generateRandomBlob(int size)
    {
        byte[] chunk = new byte[size];
        for (int k = 0; k < chunk.length; k++)
        {
            chunk[k] = (byte) (Math.random() * 255);
        }
        return chunk;
    }

    public static byte[] generateZeroBlob(int size)
    {
        byte[] chunk = new byte[size];
        for (int k = 0; k < chunk.length; k++)
        {
            chunk[k] = (byte) 0;
        }
        return chunk;
    }

    public static byte[] generateRandomChunk(int sizeFrom, int sizeTo)
    {
        Random staticRand = new Random(System.currentTimeMillis());
        int size = sizeFrom + staticRand.nextInt(sizeTo - sizeFrom);
        return generateRandomBlob(size);
    }
}
