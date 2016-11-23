package cn.v5.mr;

import org.springframework.beans.factory.FactoryBean;

public class MRPublisherFactoryBean implements FactoryBean<MRPublisher> {

    private MRConnectionManager connectionManager;

    private int priority;

    public void setConnectionManager(MRConnectionManager connectionManager) {
        this.connectionManager = connectionManager;
    }


    public void setPriority(int priority) {
        this.priority = priority;
    }

    @Override
    public MRPublisher getObject() throws Exception {
        MRPublisher publisher = connectionManager.getMRPublisher( priority );
        return publisher;
    }

    @Override
    public Class<?> getObjectType() {
        return MRPublisher.class;
    }

    @Override
    public boolean isSingleton() {
        return true;
    }
}
