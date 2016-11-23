package cn.v5.mr.impl;

import cn.v5.mr.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

public class MRConnectionManagerImpl implements MRConnectionManager {
    private static Logger logger = LoggerFactory.getLogger(MRConnectionManagerImpl.class);

    private MRClient client;
    private Set<MRSubscriber> subscribers = new LinkedHashSet<>();

    private ConcurrentHashMap<Integer, MRPublisher> publisherCache = new ConcurrentHashMap<>();

    private Map<String, MRMessageListener> listenerMap = new HashMap<>();

    protected String url;
    protected Executor executor;
    protected int perfetchSize;
    protected int timeout;


    public void setPerfetchSize(int perfetchSize) {
        this.perfetchSize = perfetchSize;
    }

    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    protected boolean init() {
        if (client != null && client.isRunning()) {
            if (logger.isWarnEnabled()) {
                logger.warn("mr client is running.");
            }
            return false;
        } else {
            if (logger.isInfoEnabled()) {
                logger.info("init url {}", this.url);
            }
            client = new MRClientJNIImpl(this.url, this.executor);
            client.start();
            return true;
        }
    }

    @Override
    public boolean init(String url, Executor executor) {
        if (client != null && client.isRunning()) {
            if (logger.isWarnEnabled()) {
                logger.warn("mr client is running.");
            }
            return false;
        } else {
            this.url = url;
            this.executor = executor;
            return init();
        }
    }

    @Override
    public void shutdown() {
        try {
            shutdownAndWait(0);
        } catch (InterruptedException e) {
            logger.error(e.getMessage(), e);
        }
    }

    @Override
    public void shutdownAndWait() throws InterruptedException {
        shutdownAndWait(-1);
    }

    @Override
    public void shutdownAndWait(int ms) throws InterruptedException {
        if (client != null && client.isRunning()) {
            if (logger.isInfoEnabled()) {
                logger.info("shutdown url {}", this.url);
            }
            subscribers.forEach(MRSubscriber::unSub);
            client.stopAndWait(ms);
        } else {
            if (logger.isWarnEnabled()) {
                logger.warn("mr client is null or no running.");
            }
        }
    }

    protected MRSubscriber getMRSubscriber(String topic, int pfSize, int timeout, MRMessageListener listener) {
        if (topic == null && listener == null) {
            if (logger.isErrorEnabled()) {
                logger.error("topic {} is null or listener is null", topic);
            }
            return null;
        }
        MRSubscriber subscriber = new MRSubscriberImpl(client, topic, pfSize, listener);
        subscribers.add(subscriber);
        client.sub(topic, pfSize, timeout, subscriber);
        return subscriber;
    }

    @Override
    public MRPublisher getMRPublisher(int priority) {
        return publisherCache.computeIfAbsent(priority, integer -> new MRPublisherImpl(client, priority));
    }

    @Override
    public void addListener(String topic, MRMessageListener listener) {
        addListener(topic, perfetchSize, listener);
    }

    @Override
    public void addListener(String topic, int pfSize, MRMessageListener listener) {
        addListener(topic, pfSize, timeout, listener);
    }

    @Override
    public void addListener(String topic, int pfSize, int timeout, MRMessageListener listener) {
        MRSubscriber subscriber = getMRSubscriber(topic,
                pfSize == 0 ? perfetchSize : pfSize,
                timeout == 0 ? this.timeout : timeout, listener);
        if (subscriber == null) {
            logger.error("add listener for topic {} fail.", topic);
        } else {
            listenerMap.put(topic, listener);
        }
    }

    @Override
    public void addListeners(Map<String, MRMessageListener> listenerMap) {
        if (listenerMap != null && listenerMap.size() > 0) {
            listenerMap.forEach(this::addListener);
        } else {
            logger.warn("listeners map is null");
        }
    }


    @Override
    public boolean containsListener(String topic) {
        return listenerMap != null && listenerMap.containsKey(topic);
    }

    @Override
    public MRClient getMRClient() {
        return client;
    }

}
