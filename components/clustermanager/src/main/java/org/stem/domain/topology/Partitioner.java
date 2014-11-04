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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Map;

public abstract class Partitioner {

    public static enum Type {
        CRUSH("crush", Crush.builder);

        private static Map<String, Type> values = new HashMap<>();

        static {
            for (Type val : Type.values()) {
                values.put(val.name, val);
            }
        }

        @JsonCreator
        public static Type byName(String name) {
            if (null == name)
                throw new IllegalStateException("Partition name is empty");

            Type type = values.get(name);
            if (null == type)
                throw new IllegalStateException(String.format("Invalid partitioner name: %s. Available partitioners: %s",
                        name, StringUtils.join(values.keySet(), ", ")));

            return type;
        }

        @JsonValue
        public String getName() {
            return name;
        }

        final String name;
        public final Builder builder;

        Type(String name, Builder builder) {
            this.name = name;
            this.builder = builder;
        }
    }

    public static abstract class Builder {

        public abstract Partitioner build();
    }

    public abstract AlgorithmAdapter algorithm();

    /**
     *
     */
    public static class Crush extends Partitioner { // TODO: singleton?
        public static final CrushAdapter algorithm = new CrushAdapter();
        private static final Partitioner.Builder builder = new Builder();

        @Override
        public AlgorithmAdapter algorithm() {
            return algorithm;
        }

        public static class Builder extends Partitioner.Builder {

            @Override
            public Partitioner build() {
                return new Crush();
            }
        }
    }
}
