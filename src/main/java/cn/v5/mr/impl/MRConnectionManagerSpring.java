package cn.v5.mr.impl;

import cn.v5.mr.MRMessageListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;

import java.util.Map;
import java.util.concurrent.Executor;


public class MRConnectionManagerSpring extends MRConnectionManagerImpl implements InitializingBean, DisposableBean {
    private static Logger logger = LoggerFactory.getLogger(MRConnectionManagerSpring.class);

    private Map<String, MRMessageListener> messageListenerMap;

    @Override
    public void afterPropertiesSet() throws Exception {
        if (init()){
            addListeners(messageListenerMap);
        }else{
            logger.error("mr connection manager init fail.");
        }
    }

    @Override
    public void destroy() throws Exception {
        shutdown();
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public void setExecutor(Executor exector) {
        this.executor = exector;
    }

    public void setMessageListenerMap(Map<String, MRMessageListener> messageListenerMap) {
        this.messageListenerMap = messageListenerMap;
    }
}
