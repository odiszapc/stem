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

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.DowngradingConsistencyRetryPolicy;
import com.datastax.driver.core.policies.ExponentialReconnectionPolicy;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.stem.domain.ExtendedBlobDescriptor;
import org.stem.domain.Schema;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

public class MetaStoreClient {

    private static final String INSERT_STATEMENT = "INSERT INTO stem.blobs_meta (blob, disk, data) VALUES (?, ?, ?)";
    private static final String SELECT_STATEMENT = "SELECT * FROM stem.blobs_meta WHERE blob = ?";
    private static final String SELECT_REPLICA_STATEMENT = "SELECT * FROM stem.blobs_meta WHERE blob = ? AND disk = ?";
    private static final String UPDATE_STATEMENT = "UPDATE stem.blobs_meta SET data = ? WHERE blob = ? AND disk = ?";
    private static final String DELETE_STATEMENT = "DELETE FROM stem.blobs_meta WHERE blob = ?";
    private static final String DELETE_REPLICA_STATEMENT = "DELETE FROM stem.blobs_meta WHERE blob=? AND disk=?";

    protected Cluster cluster;
    protected Session session;
    private volatile boolean started;

    PreparedStatement insertToBlobsMeta;
    PreparedStatement selectBlobsMeta;
    PreparedStatement selectReplicaBlobsMeta;
    PreparedStatement updateBlobsMeta;
    PreparedStatement deleteBlobsMeta;
    PreparedStatement deleteReplicaBlobsMeta;


    public MetaStoreClient(String... contactPoints) {
        cluster = Cluster.builder()
                .addContactPoints(contactPoints)
                .withClusterName(Schema.CLUSTER)
                .withLoadBalancingPolicy(new RoundRobinPolicy())
                .withRetryPolicy(DowngradingConsistencyRetryPolicy.INSTANCE)
                .withReconnectionPolicy(new ExponentialReconnectionPolicy(100, 10000))
                .withoutMetrics()
                .build();
    }

    public void start() {
        start(true);
    }

    protected void start(boolean setKeyspace) {
        if (started)
            throw new IllegalStateException("Cassandra client is already started");


        session = cluster.connect(Schema.KEYSPACE);

        if (setKeyspace)
            session.execute("use stem");

        prepareStatements();
        started = true;
    }

    public boolean isStarted() {
        return started;
    }

    private void prepareStatements() {
        insertToBlobsMeta = session.prepare(INSERT_STATEMENT);
        selectBlobsMeta = session.prepare(SELECT_STATEMENT);
        selectReplicaBlobsMeta = session.prepare(SELECT_REPLICA_STATEMENT);
        updateBlobsMeta = session.prepare(UPDATE_STATEMENT);
        deleteBlobsMeta = session.prepare(DELETE_STATEMENT);
        deleteReplicaBlobsMeta = session.prepare(DELETE_REPLICA_STATEMENT);
    }

    public void stop() {
        cluster.close();
    }

    public Session getSession() {
        return session;
    }

    public List<ExtendedBlobDescriptor> readMeta(byte[] key) {
        BoundStatement statement = selectBlobsMeta.bind(ByteBuffer.wrap(key));
        List<Row> rows = getSession().execute(statement).all();

        List<ExtendedBlobDescriptor> results = Lists.newArrayList();

        for (Row row : rows) {
            ExtendedBlobDescriptor writeResult = extractMeta(row);
            results.add(writeResult);
        }
        return results;
    }

    public ExtendedBlobDescriptor readMeta(byte[] key, UUID diskId) {
        BoundStatement statement = selectReplicaBlobsMeta.bind(ByteBuffer.wrap(key), diskId);
        Row row = getSession().execute(statement).one(); // TODO: check is there are many replicas for this particular blob and disk ??? (is it possible?)
        if (null == row)
            return null;

        return extractMeta(row);
    }

    public void writeMeta(Collection<ExtendedBlobDescriptor> results) {
        BatchStatement batch = new BatchStatement();
        for (ExtendedBlobDescriptor wr : results) {
            ByteBuffer key = ByteBuffer.wrap(wr.getKey());
            ByteBuffer data = buildMeta(wr.getFFIndex(), wr.getBodyOffset(), wr.getLength());

            batch.add(insertToBlobsMeta.bind(key, wr.getDisk(), data));
        }
        ResultSet execute = session.execute(batch);
        // TODO: continue here
    }

    public void updateMeta(ExtendedBlobDescriptor d) {
        updateMeta(d.getKey(), d.getDisk(), d.getFFIndex(), d.getBodyOffset(), d.getLength());
    }

    public void updateMeta(byte[] key, UUID diskId, int fatFileIndex, int offset, int length) {
        // UPDATE stem.blobs_meta SET data = ? WHERE blob = ? AND disk = ?
        ByteBuffer data = buildMeta(fatFileIndex, offset, length);
        BoundStatement statement = updateBlobsMeta.bind(data, ByteBuffer.wrap(key), diskId);
        session.execute(statement);
    }

    public void deleteMeta(byte[] key) {
        BoundStatement statement = deleteBlobsMeta.bind(ByteBuffer.wrap(key));
        getSession().execute(statement);
    }

    private ByteBuffer buildMeta(int fatFileIndex, int offset, int length) {
        ByteBuf b = Unpooled.buffer(12);
        b.writeInt(fatFileIndex);
        b.writeInt(offset);
        b.writeInt(length);

        return b.nioBuffer();
    }

    private ExtendedBlobDescriptor extractMeta(Row row) {
        ByteBuffer keyBuf = row.getBytes("blob");
        byte[] key = new byte[keyBuf.remaining()];
        keyBuf.get(key, 0, key.length);

        UUID disk = row.getUUID("disk");
        ByteBuffer buf = row.getBytes("data");
        int fatFileIndex = buf.getInt();
        int offset = buf.getInt();
        int length = buf.getInt();

        return new ExtendedBlobDescriptor(key, length, disk, fatFileIndex, -1, offset);
    }


    public void deleteReplica(byte[] key, UUID diskId) {
        BoundStatement statement = deleteReplicaBlobsMeta.bind(ByteBuffer.wrap(key), diskId);
        getSession().execute(statement);
    }
}
