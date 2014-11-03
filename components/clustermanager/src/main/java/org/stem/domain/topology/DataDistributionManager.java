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

package org.stem.domain.topology;

import java.util.HashMap;
import java.util.Map;

public abstract class DataDistributionManager {

    public static enum Algorithm {
        CRUSH("crush", CRUSHDistributionManager.adapter);

        private static Map<String, Algorithm> values = new HashMap<>();

        static {
            for (Algorithm val : Algorithm.values()) {
                values.put(val.name, val);
            }
        }

        public static Algorithm byName(String name) {
            Algorithm algo = values.get(name);
            if (null == algo)
                throw new IllegalStateException(String.format("Unknown algorithm \"%s\"", name));
            return algo;
        }

        final String name;
        final AlgorithmAdapter adapter;

        Algorithm(String name, AlgorithmAdapter adapter) {
            this.name = name;
            this.adapter = adapter;
        }
    }
}
