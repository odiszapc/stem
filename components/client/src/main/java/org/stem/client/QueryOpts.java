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

public class QueryOpts {

    public static final Consistency.Level DEFAULT_CONSISTENCY = Consistency.Level.QUORUM;

    private volatile Consistency.Level consistency = DEFAULT_CONSISTENCY;
    private volatile Consistency.Merge mergeMethod = Consistency.Merge.RANDOM;

    public QueryOpts() {
    }

    public QueryOpts setConsistency(Consistency.Level consistencyLevel) {
        this.consistency = consistencyLevel;
        return this;
    }

    public Consistency.Level getConsistency() {
        return consistency;
    }

    public Consistency.Merge getMergeMethod() {
        return mergeMethod;
    }

    public void setMergeMethod(Consistency.Merge mergeMethod) {
        this.mergeMethod = mergeMethod;
    }
}
