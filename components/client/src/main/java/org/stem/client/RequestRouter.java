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

import java.util.Set;
import java.util.UUID;

public class RequestRouter {

    private final Session session;

    RequestRouter(Session session) {
        this.session = session;
    }

    Host getHost(Message.Request request) {
        if (request instanceof Requests.DestinationMatching) {
            Object routingKey = ((Requests.DestinationMatching) request).getRoutingKey();
            if (null == routingKey)
                throw new NoHostAvailableException(String.format("Routing key is null"));

            Host host = getHostForRoutingKey(routingKey);
            if (null == host)
                throw new NoHostAvailableException(String.format("No host found for routing key %s", routingKey));

            return host;
        } else {
            return null; // It's up to QueryPlan;
        }
    }

    private Metadata metadata() {
        return session.cluster.manager.metadata;
    }

    private Host getHostForRoutingKey(Object routingKey) {
        if (routingKey instanceof UUID) {
            UUID disk = (UUID) routingKey;
            return metadata().findHostForDisk(disk);
        }

        return null; // TODO: another routing key?
    }

    Set<Host> getEndpointsForBlob(Blob blob) {
        return metadata().getHostsForBlob(blob);
    }

    Set<UUID> getLocationsForBlob(Blob blob) {
        Set<UUID> locations = metadata().getLocationsForBlob(blob);

        if (null == locations)
            throw new NoHostAvailableException(blob.toString());
        return locations;
    }
}
