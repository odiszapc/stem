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

import org.stem.coordination.StemZooEventHandler;
import org.stem.coordination.TopoMapping;

import java.util.ArrayList;
import java.util.List;

public class TopoMapListener extends StemZooEventHandler<TopoMapping> {

    List<TopoMapSubscriber> subscribers = new ArrayList<TopoMapSubscriber>();

    @Override
    public Class<? extends TopoMapping> getBaseClass() {
        return TopoMapping.class;
    }

    @Override
    protected synchronized void onNodeUpdated(TopoMapping mapping) {
        for (TopoMapSubscriber subscriber : subscribers) {
            subscriber.mappingChanged(mapping);
        }
    }

    public void listen(TopoMapSubscriber subscriber) {
        subscribers.add(subscriber);
    }
}
