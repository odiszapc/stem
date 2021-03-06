/*
 * Copyright 2015 Alexey Plotnik
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

package org.stem.api.resources;

import org.stem.RestUtils;
import org.stem.api.RESTConstants;
import org.stem.api.UserMessages;
import org.stem.api.request.NewStreamingSessionRequest;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path(RESTConstants.Api.Streaming.URI)
@Produces(MediaType.APPLICATION_JSON)
public class StreamingResource {

    @POST
    @Path(RESTConstants.Api.Streaming.New.BASE)
    public Response create(NewStreamingSessionRequest req) {
        return RestUtils.ok(UserMessages.STREAMING_SESSION_STARTED);
    }
}
