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


public class BalancerTest {

    @Test
    public void testArrayBalancer() throws Exception {
        ArrayBalancer balancer = new ArrayBalancer(100000);
        String hash = "abababababababababababababababab";
        hash = "ffffffffffffffffffffffffffffffff";
        Integer token = balancer.getToken(hash);

        Assert.assertEquals(100000, token.longValue());
        hash = "00000000000000000000000000000001";
        token = balancer.getToken(hash);
        Assert.assertEquals(1, token.longValue());
    }
}
