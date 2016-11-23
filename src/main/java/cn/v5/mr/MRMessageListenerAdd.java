package cn.v5.mr;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;

public class MRMessageListenerAdd implements InitializingBean, DisposableBean {
    private MRMessageListener listener;
    private MRConnectionManager connectionManager;
    private String topic;

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public void setListener(MRMessageListener listener) {
        this.listener = listener;
    }

    public void setConnectionManager(MRConnectionManager connectionManager) {
        this.connectionManager = connectionManager;
    }

    @Override
    public void destroy() throws Exception {
        listener = null;
        connectionManager = null;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        this.connectionManager.addListener(topic, listener);
    }
}
