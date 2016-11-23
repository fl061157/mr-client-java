package cn.v5.mr.impl;

import cn.v5.mr.MRClient;
import cn.v5.mr.MRPublisher;
import cn.v5.mr.MessageCallback;
import cn.v5.mr.MessageResultContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Future;

public class MRPublisherImpl implements MRPublisher {
    private static Logger logger = LoggerFactory.getLogger(MRPublisherImpl.class);

    private MRClient client;

    private int priority;


    public MRPublisherImpl(MRClient client, int priority) {
        this.client = client;
        this.priority = priority;
    }

    public MRPublisherImpl(MRClient client) {
        this(client, 1);
    }


    @Override
    public boolean syncPub(String topic, byte[] data) {
        return client.pub(topic, data, priority) == 0;
    }

    @Override
    public boolean syncPubDelay(String topic, byte[] data, int sec) {
        return client.pubDelay(topic, data, sec, priority) == 0;
    }

    @Override
    public boolean syncPubSort(String topic, byte[] data, String sortKey) {
        return client.pubSort(topic, data, sortKey, priority) == 0;
    }

    @Override
    public MessageResultContext syncPubDirect(String topic, byte[] data) {
        MessageResultContext mrc = new MessageResultContext();
        int ret = client.pubDirect(topic, data, mrc, priority);
        if (ret != 0) {
            mrc.setResult(-1);
        }
        return mrc;
    }

    @Override
    public Future<MessageResultContext> asyncPub(String topic, byte[] data) {
        logger.warn("unsupported");
        return null;
    }

    @Override
    public boolean asyncPub(String topic, byte[] data, MessageCallback callback) {
        return client.pub(topic, data, callback, priority);
    }

    @Override
    public boolean asyncPubDelay(String topic, byte[] data, int sec, MessageCallback callback) {
        return client.pubDelay(topic, data, sec, callback, priority);
    }

    @Override
    public boolean asyncPubSort(String topic, byte[] data, String sortKey, MessageCallback callback) {
        return client.pubSort(topic, data, sortKey, callback, priority);
    }

    @Override
    public boolean asyncPubDirect(String topic, byte[] data, MessageCallback callback) {
        return client.pubDirect(topic, data, callback, priority);
    }

    @Override
    public boolean unsafePub(String topic, byte[] data) {
        return client.pubUnsafe(topic, data, priority);
    }


    @Override
    public void setPriority(int priority) {
        this.priority = priority;
    }
}
