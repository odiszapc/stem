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

package org.stem.api;

/**
 * REST resources:
 * <p/>
 * POST /cluster/init
 * GET  /cluster
 * POST /cluster/join
 * GET  /cluster/unauthorized
 * POST /cluster/authorize
 */
public class RESTConstants {

    public static class Api {

        public static final String BASE = "/api";

        public static class Cluster {

            public static final String BASE = "/cluster";
            public static final String URI = Api.BASE + BASE;

            public static class Init {

                public static final String BASE = "/init";
                public static final String URI = Cluster.URI + BASE;
            }

            public static class Join {

                public static final String BASE = "/join";
                public static final String URI = Cluster.URI + BASE;
            }

            public static class Get {

                public static final String BASE = "/";
                public static final String URI = Cluster.URI + BASE;
            }

            public static class Unauthorized {

                public static final String BASE = "/unauthorized";
                public static final String URI = Cluster.URI + BASE;
            }

            public static class Approve {

                public static final String BASE = "/authorize";
                public static final String URI = Cluster.URI + BASE;
            }

            public static class Refuse {

                public static final String BASE = "/refuse";
                public static final String URI = Cluster.URI + BASE;
            }
        }

        public static class Topology {

            public static final String BASE = "/topology";
            public static final String URI = Api.BASE + BASE;

            public static class Get {

                public static final String BASE = "/";
                public static final String URI = Topology.URI + BASE;
            }

            public static class Build {

                public static final String BASE = "/build";
                public static final String URI = Topology.URI + BASE;
            }
        }

    }
}
