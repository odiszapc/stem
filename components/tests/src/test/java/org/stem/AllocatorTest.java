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

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.stem.db.FatFileAllocator;
import org.stem.utils.TestUtils;

import java.io.File;

import static org.stem.utils.TestUtils.temporize;

public class AllocatorTest {

    @Before
    public void setUp() throws Exception {
        TestUtils.cleanupTempDir();
        TestUtils.createTempDir();
    }

    @After
    public void tearDown() throws Exception {
        TestUtils.cleanupTempDir();
    }

    @Test
    public void testSize() throws Exception {
        String fatFilePath = temporize("000001.db");
        FatFileAllocator.allocateFile(fatFilePath, 256);

        File file = new File(fatFilePath);
        Assert.assertEquals(file.length(), 256 * 1024 * 1024);
    }

    @Test
    public void testAllocateDirectoryMaxSize() throws Exception {
        String directoryPath = temporize("AllocateDirectoryMaxSize");
        File dir = new File(directoryPath);
        Assert.assertTrue(dir.mkdir());
        FatFileAllocator.allocateDirectory(directoryPath, 5, 30 * 1024 * 1024, false);
    }
}
