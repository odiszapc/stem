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

package org.stem.coordination;

import com.google.common.util.concurrent.AbstractFuture;
import org.stem.api.response.StemResponse;

import java.util.UUID;

public class EventFuture extends AbstractFuture<StemResponse> implements Event.Handler.Callback {

    private final Event event;
    private final ZookeeperClient client;

    public EventFuture(Event event, ZookeeperClient client) {
        this.event = event;
        this.client = client;
    }

    public UUID eventId() {
        return event.id;
    }

    public void setResult(StemResponse response) {
        onSet(response, -1);
    }

    @Override
    public void onSet(StemResponse result, long latency) {
        try {
            //EventManager.this.updateEvent(event.id, result);
            event.setResponse(result);
            client.updateNode(ZookeeperPaths.ASYNC_REQUESTS, event);
        } catch (Exception e) {
            setException(e);
            return;
        }
        set(result);
    }
}
