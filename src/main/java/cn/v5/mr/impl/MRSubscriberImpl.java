package cn.v5.mr.impl;

import cn.v5.mr.MRClient;
import cn.v5.mr.MRMessageListener;
import cn.v5.mr.MRSubscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MRSubscriberImpl implements MRSubscriber {
    private static Logger logger = LoggerFactory.getLogger(MRSubscriberImpl.class);

    private MRClient client;
    private String topic;
    private MRMessageListener messageListener;
    private int perfetchSize;

    public MRSubscriberImpl(MRClient c, String topic, int pfsize, MRMessageListener listener){
        this.client = c;
        this.topic = topic;
        this.messageListener = listener;
        this.perfetchSize = pfsize;
    }

    @Override
    public void ackOk(long messageId) {
        client.ack(topic, messageId);
    }

    @Override
    public void ackOk(long messageId, byte[] data) {
        client.ack(topic, messageId, 0, data);
    }

    @Override
    public void ackFail(long messageId) {
        client.ack(topic, messageId, 1);
    }

    @Override
    public void unSub() {
        client.unsub(topic);
    }

    @Override
    public void on(int status, long mid, byte[] bytes) {
        if (messageListener != null){
            try {
                messageListener.onMessage(this, mid, bytes);
            }catch (Exception e){
                logger.error("onMessage error {}", e.getMessage(), e);
            }
        }else{
            logger.error("no listener for topic {}", topic);
        }
    }
}
