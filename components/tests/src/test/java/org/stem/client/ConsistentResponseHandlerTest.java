package org.stem.client;

import org.easymock.IMocksControl;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.stem.client.Message.Request;
import org.stem.client.Requests.DeleteBlob;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.easymock.EasyMock.*;

public class ConsistentResponseHandlerTest {
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    IMocksControl ctrl;
    Session session;

    Consistency.Level consistency = Consistency.Level.QUORUM;
    long sessionTimeoutMs = 10;

    @Before
    public void before() {
        this.ctrl = createControl();
        final Metadata metadata = this.ctrl.createMock(Metadata.class);
        Host host = new Host(InetSocketAddress.createUnresolved("blablabla.com", 993));
        expect(metadata.getHost(isA(InetSocketAddress.class))).andReturn(host).anyTimes();
        RequestRouter router = this.ctrl.createMock(RequestRouter.class);
        expect(router.getHost(isA(Request.class))).andReturn(host).anyTimes();
        this.session = new Session(null, router) {
            @Override
            public Metadata getClusterMetadata() {
                return metadata;
            }
        };
    }

    @Test(timeout = 500)
    public void shouldTimeoutIfFuturesWeAreWaitingForAreBlockingForTooMuchTime() throws Exception {
        long sleepMs = this.sessionTimeoutMs + 2000;
        List<DefaultResultFuture> futures = Arrays.asList(createFutureSleeping(this.ctrl, sleepMs));
        this.ctrl.replay();

        ConsistentResponseHandler handler = new ConsistentResponseHandler(this.session, futures, this.consistency, this.sessionTimeoutMs);

        this.expectedException.expectMessage("Response is inconsistent");
        handler.waitFor();
    }

    @Test(timeout = 500)
    public void shouldThrowOnWaitForIfConsistencyCanNotBeAchieved() throws Exception {
        List<DefaultResultFuture> futures = new ArrayList<>();
        for (int i=0; i<5; i++) {
            futures.add(createResultFuture(this.ctrl));
        }
        for (int i=0; i<5; i++) {
            futures.add(createErrorFuture(this.ctrl));
        }
        this.ctrl.replay();

        ConsistentResponseHandler handler = new ConsistentResponseHandler(this.session, futures, this.consistency, this.sessionTimeoutMs);

        this.expectedException.expectMessage("Response is inconsistent");
        handler.waitFor();
    }

    @Test(timeout = 500)
    public void shouldOkIfConsistencyAchieved() throws Exception {
        List<DefaultResultFuture> futures = new ArrayList<>();
        for (int i=0; i<5; i++) {
            futures.add(createResultFuture(this.ctrl));
        }
        this.ctrl.replay();

        ConsistentResponseHandler handler = new ConsistentResponseHandler(this.session, futures, this.consistency, this.sessionTimeoutMs);

        this.expectedException.expectMessage("Response is inconsistent");
        handler.waitFor();
    }


    private DefaultResultFuture createFutureSleeping(IMocksControl ctrl, final long sleepMs) throws InterruptedException, ExecutionException {
        Message.Request request = new DeleteBlob(UUID.randomUUID(), 1, 2);
        DefaultResultFuture future = new DefaultResultFuture(null, request);
        return future;
    }

    private DefaultResultFuture createErrorFuture(IMocksControl ctrl) throws Exception {
        DefaultResultFuture future = ctrl.createMock("errorFuture", DefaultResultFuture.class);
        Message.Request request = new DeleteBlob(UUID.randomUUID(), 1, 2);
        expect(future.request()).andReturn(request).anyTimes();
        expect(future.get()).andThrow(new ExecutionException("Oops", new RuntimeException()));
        expect(future.get(anyLong(), isA(TimeUnit.class))).andThrow(new ExecutionException("Oops", new RuntimeException()));
        return future;
    }

    private DefaultResultFuture createResultFuture(IMocksControl ctrl) throws Exception {
        DefaultResultFuture future = ctrl.createMock("resultFuture", DefaultResultFuture.class);
        Message.Request request = new DeleteBlob(UUID.randomUUID(), 1, 2);
        expect(future.request()).andReturn(request).anyTimes();
        expect(future.get()).andReturn(null);
        expect(future.get(anyLong(), isA(TimeUnit.class))).andReturn(null);
        return future;
    }
}
