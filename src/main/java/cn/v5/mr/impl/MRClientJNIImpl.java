package cn.v5.mr.impl;

import cn.v5.mr.MRClient;
import cn.v5.mr.MessageCallback;
import cn.v5.mr.MessageResultContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executor;

final class MRClientJNIImpl implements MRClient {

    private static Logger logger = LoggerFactory.getLogger(MRClientJNIImpl.class);

    private volatile long clientPtr;
    private String url;
    private Executor executor;
    private int timeout;

    static {
        System.loadLibrary("mr");
    }

    final static int PUB_ACK_TYPE_NONE = 0;
    final static int PUB_ACK_TYPE_SERVER = 1;
    final static int PUB_ACK_TYPE_PEER = 2;

    final static int PUB_QOS_NONE = 0;
    final static int PUB_QOS_NO_LOSS = 1;
    final static int PUB_QOS_NO_DUP = 2;

    final static int PERFETCH_SIZE_DEFAULT = 1024;
    final static int PERFETCH_SIZE_MAX = 8192;

    private native long nativeCreate(String url, int timeoutSec);

    private native boolean nativeStart(long clientPtr);

    private native boolean nativeStop(long clientPtr, int wait);

    private native long nativeRetain(long clientPtr);

    private native boolean nativeRelease(long clientPtr);

    private native boolean nativeIsRunning(long clientPtr);

    private native boolean nativeIsClosed(long clientPtr);

    private native boolean nativeProcessMessageResultContext(long clientPtr);

    private native boolean nativeAsyncPub(long clientPtr, String topic, byte[] bytes, MessageCallback mcb,
                                          int ackType, int qos, int order, String orderKey,
                                          int alive, int delay, int interval, int repeat, int timeout, int priority);

    private native int nativeSyncPub(long clientPtr, String topic, byte[] bytes,
                                     int ackType, int qos, int order, String orderKey,
                                     int alive, int delay, int interval, int repeat, int timeout,
                                     MessageResultContext mrc, int priority);

    private native boolean nativeRegisterPub(long clientPtr, String topic);

    private native boolean nativeRegisterSub(long clientPtr, String topic, int ackType, int perfetchSize, int timeout, MessageCallback mcb);

    private native boolean nativeAck(long clientPtr, String topic, long mid, int status, byte[] bytes);

    private native boolean nativeUnregister(long clientPtr, String topic);

    private native boolean nativeCancel(long clientPtr, String topic, long mid);

    private MRClientJNIImpl() {

    }

    public MRClientJNIImpl(String url, Executor executor) {
        this(url, 2, executor);
    }

    public MRClientJNIImpl(String url, int timeoutSec, Executor executor) {
        this.url = url;
        this.executor = executor;
        this.timeout = timeoutSec;
    }

    @Override
    protected void finalize() throws Throwable {
        if (clientPtr>0) {
            nativeRelease(clientPtr);
        }
        super.finalize();
    }

    @Override
    public synchronized void start() {
        if (clientPtr > 0) {
            logger.error("mr client is running, can't start again.");
            return;
        }
        clientPtr = nativeCreate(url, timeout);
        nativeStart(clientPtr);
        nativeRetain(clientPtr);

        final long cptr = nativeRetain(clientPtr);
        Thread t = new Thread(() -> {
            if (logger.isInfoEnabled()) {
                logger.info("Pub callback thread start.");
            }

            nativeProcessMessageResultContext(cptr);

            nativeRelease(cptr);

            if (logger.isInfoEnabled()) {
                logger.info("Pub callback thread end.");
            }
        }, "MR Callback");
        t.start();
    }

    public void processMessageResultContext(final MessageResultContext prc) {
        if (prc != null) {
            try {
                executor.execute(() -> {
                    MessageCallback pc = prc.getPubCallback();
                    if (pc != null) {
                        try {
                            pc.on(prc.getResult(), prc.getMessageId(), prc.getBytes());
                        } catch (Exception e) {
                            logger.error(e.getMessage(), e);
                        }
                    }
                });
            } catch (Throwable e) {
                logger.error(e.getMessage(), e);
            }
        }
    }

    @Override
    public synchronized void stop() {
        try {
            stopAndWait(0);
        } catch (InterruptedException e) {
            logger.error(e.getMessage(), e);
        }
    }

    @Override
    public synchronized void stopAndWait() throws InterruptedException {
        stopAndWait(-1);
    }

    @Override
    public synchronized void stopAndWait(int ms) throws InterruptedException {
        if (clientPtr < 1 || !nativeIsRunning(clientPtr)) {
            logger.error("mr client is not running, can't stop.");
            return;
        }
        nativeStop(clientPtr, ms);
        if (!nativeIsClosed(clientPtr)){
            throw new InterruptedException("mr client close wait interrupted.");
        }
    }

    @Override
    public synchronized boolean isRunning() {
        return nativeIsRunning(clientPtr);
    }

    @Override
    public int pub(String topic, byte[] bytes) {
        return pub(topic, bytes, 1);
    }

    public int pub(String topic, byte[] bytes, int priority) {
        return nativeSyncPub(clientPtr, topic, bytes,
                PUB_ACK_TYPE_SERVER, PUB_QOS_NO_LOSS, 0, null, 0, 0, 0, 0, 0, null, priority);
    }


    @Override
    public int pubDelay(String topic, byte[] bytes, int delay_secs) {
        return pubDelay(topic, bytes, delay_secs, 1);
    }

    public int pubDelay(String topic, byte[] bytes, int delay_secs, int priority) {
        return nativeSyncPub(clientPtr, topic, bytes,
                PUB_ACK_TYPE_SERVER, PUB_QOS_NO_LOSS, 0, null, 0, delay_secs, 0, 0, 0, null, priority);
    }


    @Override
    public int pubSort(String topic, byte[] bytes, String sortKey) {
        return pubSort(topic, bytes, sortKey, 1);
    }

    public int pubSort(String topic, byte[] bytes, String sortKey, int priority) {
        return nativeSyncPub(clientPtr, topic, bytes,
                PUB_ACK_TYPE_SERVER, PUB_QOS_NO_LOSS, 1, sortKey, 0, 0, 0, 0, 0, null, priority);
    }


    @Override
    public int pubRepeat(String topic, byte[] bytes, int interval, int times) {
        return pubRepeat(topic, bytes, interval, times, 1);
    }

    @Override
    public int pubRepeat(String topic, byte[] bytes, int interval, int times, int priority) {
        return nativeSyncPub(clientPtr, topic, bytes,
                PUB_ACK_TYPE_SERVER, PUB_QOS_NO_LOSS, 0, null, 0, 0, interval, times, 0, null, priority);
    }

    @Override
    public int pubDirect(String topic, byte[] bytes, MessageResultContext mrc) {
        return pubDirect(topic, bytes, mrc, 1);
    }

    @Override
    public int pubDirect(String topic, byte[] bytes, MessageResultContext mrc, int priority) {
        return nativeSyncPub(clientPtr, topic, bytes,
                PUB_ACK_TYPE_PEER, PUB_QOS_NO_LOSS, 0, null, 0, 0, 0, 0, 0, mrc, priority);
    }

    @Override
    public boolean pub(String topic, byte[] bytes, MessageCallback pcb) {
        return pub(topic, bytes, pcb, 1);
    }

    @Override
    public boolean pub(String topic, byte[] bytes, MessageCallback pcb, int priority) {
        return nativeAsyncPub(clientPtr, topic, bytes, pcb,
                PUB_ACK_TYPE_SERVER, PUB_QOS_NO_LOSS, 0, null, 0, 0, 0, 0, 0, priority);
    }

    @Override
    public boolean pubDelay(String topic, byte[] bytes, int delay_secs, MessageCallback pcb) {
        return pubDelay(topic, bytes, delay_secs, pcb, 1);
    }

    @Override
    public boolean pubDelay(String topic, byte[] bytes, int delay_secs, MessageCallback pcb, int priority) {
        return nativeAsyncPub(clientPtr, topic, bytes, pcb,
                PUB_ACK_TYPE_SERVER, PUB_QOS_NO_LOSS, 0, null, 0, delay_secs, 0, 0, 0, priority);
    }

    @Override
    public boolean pubSort(String topic, byte[] bytes, String sortKey, MessageCallback pcb) {
        return pubSort(topic, bytes, sortKey, pcb, 1);
    }

    @Override
    public boolean pubSort(String topic, byte[] bytes, String sortKey, MessageCallback pcb, int priority) {
        return nativeAsyncPub(clientPtr, topic, bytes, pcb,
                PUB_ACK_TYPE_SERVER, PUB_QOS_NO_LOSS, 1, sortKey, 0, 0, 0, 0, 0, priority);
    }

    @Override
    public boolean pubRepeat(String topic, byte[] bytes, int interval, int times, MessageCallback pcb) {
        return pubRepeat(topic, bytes, interval, times, pcb, 1);
    }

    @Override
    public boolean pubRepeat(String topic, byte[] bytes, int interval, int times, MessageCallback pcb, int priority) {
        return nativeAsyncPub(clientPtr, topic, bytes, pcb,
                PUB_ACK_TYPE_SERVER, PUB_QOS_NO_LOSS, 0, null, 0, 0, interval, times, 0, priority);
    }

    @Override
    public boolean pubDirect(String topic, byte[] bytes, MessageCallback pcb) {
        return pubDirect(topic, bytes, pcb, 1);
    }

    @Override
    public boolean pubDirect(String topic, byte[] bytes, MessageCallback pcb, int priority) {
        return nativeAsyncPub(clientPtr, topic, bytes, pcb,
                PUB_ACK_TYPE_PEER, PUB_QOS_NO_LOSS, 0, null, 0, 0, 0, 0, 0, priority);
    }

    @Override
    public boolean pub(String topic) {
        return nativeRegisterPub(clientPtr, topic);
    }

    @Override
    public boolean sub(String topic, MessageCallback pcb) {
        return nativeRegisterSub(clientPtr, topic, PUB_ACK_TYPE_SERVER, PERFETCH_SIZE_DEFAULT, 0, pcb);
    }

    @Override
    public boolean sub(String topic, int perfetchSize, MessageCallback pcb) {
        return sub(topic, perfetchSize, 0, pcb);
    }

    @Override
    public boolean sub(String topic, int perfetchSize, int timeout, MessageCallback pcb) {
        if (perfetchSize <= 0 || perfetchSize > PERFETCH_SIZE_MAX) {
            logger.warn("perfetch size '{}' err, use default '{}'", perfetchSize, PERFETCH_SIZE_DEFAULT);
            perfetchSize = PERFETCH_SIZE_DEFAULT;
        }
        return nativeRegisterSub(clientPtr, topic, PUB_ACK_TYPE_SERVER, perfetchSize, timeout, pcb);
    }

    @Override
    public boolean ack(String topic, long mid) {
        return nativeAck(clientPtr, topic, mid, 0, null);
    }

    @Override
    public boolean ack(String topic, long mid, int status) {
        return nativeAck(clientPtr, topic, mid, status, null);
    }

    @Override
    public boolean ack(String topic, long mid, int status, byte[] bytes) {
        return nativeAck(clientPtr, topic, mid, status, bytes);
    }

    @Override
    public boolean unsub(String topic) {
        return nativeUnregister(clientPtr, topic);
    }

    @Override
    public boolean cancel(String topic, long mid) {
        return nativeCancel(clientPtr, topic, mid);
    }

    @Override
    public boolean pubUnsafe(String topic, byte[] bytes) {
        return pubUnsafe(topic, bytes, 1);
    }

    @Override
    public boolean pubUnsafe(String topic, byte[] bytes, int priority) {
        return nativeSyncPub(clientPtr, topic, bytes,
                PUB_ACK_TYPE_NONE, PUB_QOS_NONE, 0, null, 0, 0, 0, 0, 0, null, priority) == 0;
    }


    @Override
    public boolean subUnsafe(String topic, MessageCallback pcb) {
        return nativeRegisterSub(clientPtr, topic, PUB_ACK_TYPE_NONE, PERFETCH_SIZE_DEFAULT, 0, pcb);
    }

    @Override
    public boolean subUnsafe(String topic, int perfetchSize, MessageCallback pcb) {
        return nativeRegisterSub(clientPtr, topic, PUB_ACK_TYPE_NONE, perfetchSize, 0, pcb);
    }

}
