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

package org.stem.client;


import org.junit.Assert;
import org.junit.Test;
import org.stem.client.v2.StreamIdPool;
import org.stem.exceptions.ConnectionBusyException;

public class StreamIdPoolTest
{

    @Test
    public void test() throws Exception
    {
        StreamIdPool pool = new StreamIdPool();
        Assert.assertEquals(pool.borrow(), 0);
        Assert.assertEquals(pool.borrow(), 1);
        pool.release(0);
        Assert.assertEquals(pool.borrow(), 0);
        Assert.assertEquals(pool.borrow(), 2);
        Assert.assertEquals(pool.borrow(), 3);
        pool.release(1);
        Assert.assertEquals(pool.borrow(), 1);
        Assert.assertEquals(pool.borrow(), 4);

        for (int i = 5; i < 128; i++)
            Assert.assertEquals(pool.borrow(), i);

        pool.release(100);
        Assert.assertEquals(pool.borrow(), 100);

        try
        {
            pool.borrow(); // Should throw busy error

            throw new AssertionError("No more IDs should be available");
        }
        catch (ConnectionBusyException e)
        {
        }
    }
}
