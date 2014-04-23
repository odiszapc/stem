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

package org.stem.net;

import org.glassfish.grizzly.http.server.CLStaticHttpHandler;
import org.glassfish.grizzly.http.server.Request;
import org.glassfish.grizzly.http.server.Response;

public class CLStaticByPassHttpHandler extends CLStaticHttpHandler
{
    /**
     * Create <tt>HttpHandler</tt>, which will handle requests
     * to the static resources resolved by the given class loader.
     *
     * @param classLoader
     * @param docRoots    the doc roots (path prefixes), which will be used
     *                    to find resources. Effectively each docRoot will be prepended
     *                    to a resource path before passing it to {@link ClassLoader#getResource(String)}.
     *                    If no <tt>docRoots</tt> are set - the resources will be searched starting
     *                    from {@link ClassLoader}'s root.
     * @throws IllegalArgumentException if one of the docRoots doesn't end with slash ('/')
     */
    public CLStaticByPassHttpHandler(ClassLoader classLoader, String... docRoots)
    {
        super(classLoader, docRoots);
    }

    @Override
    public void service(Request request, Response response) throws Exception
    {
        final String uri = getRelativeURI(request);

        if (uri == null || !handle(uri, request, response))
        {
            onMissingResource(request, response);
        }
    }
}
