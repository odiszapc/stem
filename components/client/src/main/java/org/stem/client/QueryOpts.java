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

    public static final Consistency DEFAULT_CONSISTENCY = Consistency.QUORUM;

    private volatile Consistency consistency = DEFAULT_CONSISTENCY;

    public QueryOpts() {
    }

    public QueryOpts setConsistency(Consistency consistencyLevel) {
        this.consistency = consistencyLevel;
        return this;
    }

    public Consistency getConsistency() {
        return consistency;
    }
}
