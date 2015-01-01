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

package org.stem;

import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stem.api.request.MetaStoreConfiguration;
import org.stem.client.MetaStoreClient;

import java.util.ArrayList;
import java.util.List;

public class MetaStoreInitializer extends MetaStoreClient {

    private static final Logger logger = LoggerFactory.getLogger(MetaStoreInitializer.class);

    private final String KEYSPACE = "stem";

    private static final String CREATE_KEYSPACE = "CREATE KEYSPACE IF NOT EXISTS stem " +
            "WITH REPLICATION = {\n" +
            "  'class': 'SimpleStrategy',\n" +
            "  'replication_factor': %s\n" +
            "};";

    private static final String USE_KEYSPACE = "USE stem;";
    private static final String CREATE_TABLE = "CREATE TABLE IF NOT EXISTS blobs_meta (\n" +
            "  blob blob,\n" +
            "  disk uuid,\n" +
            "  data blob,\n" +
            "  PRIMARY KEY (blob, disk)\n" +
            ")\n" +
            "WITH COMPACT STORAGE AND\n" +
            "compaction = {\n" +
            "    'class': 'LeveledCompactionStrategy',\n" +
            "    'sstable_size_in_mb': %s,\n" +
            "    'tombstone_compaction_interval': 86400,\n" +
            "    'tombstone_threshold': 0.2\n" +
            "};";

    private final List<Statement> statements = new ArrayList<>();
    private final MetaStoreConfiguration configuration;

    public MetaStoreInitializer(MetaStoreConfiguration configuration) {
        super(configuration.getContactPoints());

        this.configuration = configuration;
    }

    public void createSchema() {
        try {
            if (!isStarted())
                start();

            for (Statement statement : statements) {
                logger.info("Execute: {}", statement);
                session.execute(statement);
            }
        } catch (Exception e) {
            logger.error("Failed to create schema", e);
        }
    }

    @Override
    public void start() {
        super.start(false);
        prepareStatements();
    }

    private void prepareStatements() {
        statements.add(new SimpleStatement(String.format(CREATE_KEYSPACE, configuration.getReplicationFactor())));
        statements.add(new SimpleStatement(USE_KEYSPACE));
        statements.add(new SimpleStatement(String.format(CREATE_TABLE, configuration.getLcsSstableSizeInMb())));
    }
}
