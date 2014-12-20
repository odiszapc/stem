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

import org.junit.Assert;
import org.junit.Test;
import org.stem.domain.ArrayBalancer;
import org.stem.domain.DHT;


public class BalancerTest {

    @Test
    public void arrayBalancer() throws Exception {
        ArrayBalancer balancer = new ArrayBalancer(100000);
        String hash = "abababababababababababababababab";
        hash = "ffffffffffffffffffffffffffffffff";
        Integer token = balancer.getToken(hash);

        Assert.assertEquals(100000, token.longValue());
        hash = "00000000000000000000000000000001";
        token = balancer.getToken(hash);
        Assert.assertEquals(1, token.longValue());
    }

    @Test
    public void dhtSmall() throws Exception {
        DHT dht = new DHT(2);

        Assert.assertEquals(0, dht.getSection("01010101010101010101010101010101"));
        Assert.assertEquals(0, dht.getSection("7fffffffffffffffffffffffffffffff")); // (2^128) / 2 - 1
        Assert.assertEquals(0, dht.getSection("80000000000000000000000000000000")); // (2^128) / 2
        Assert.assertEquals(1, dht.getSection("80000000000000000000000000000001")); // (2^128) / 2 + 1
        Assert.assertEquals(1, dht.getSection("80000000000000000000000000000002")); // (2^128) / 2 + 2
        Assert.assertEquals(0, dht.getSection("00000000000000000000000000000000")); // 0
        Assert.assertEquals(0, dht.getSection("00000000000000000000000000000001")); // 0
        Assert.assertEquals(1, dht.getSection("ffffffffffffffffffffffffffffffff")); // (2^128) - 1
    }

    @Test
    public void dht() throws Exception {
        DHT balancer = new DHT(100000);
        String hash = "abababababababababababababababab";
        Integer token = balancer.getSection("ffffffffffffffffffffffffffffffff");

        Assert.assertEquals(99999, token.longValue());
        token = balancer.getSection("00000000000000000000000000000001");
        Assert.assertEquals(0, token.longValue());
    }

}
