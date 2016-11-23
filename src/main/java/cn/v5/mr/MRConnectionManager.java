package cn.v5.mr;

import java.util.Map;
import java.util.concurrent.Executor;

public interface MRConnectionManager {

    boolean init(String url, Executor executor);

    void shutdown();

    void shutdownAndWait() throws InterruptedException;

    void shutdownAndWait(int ms) throws InterruptedException;

    void addListener(String topic, MRMessageListener listener);

    void addListener(String topic, int perfetchSize, MRMessageListener listener);

    void addListener(String topic, int perfetchSize, int timeout, MRMessageListener listener);

    void addListeners(Map<String, MRMessageListener> listenerMap);

    boolean containsListener(String topic);

    MRPublisher getMRPublisher(int priority);

    MRClient getMRClient();
}
