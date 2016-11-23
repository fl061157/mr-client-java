package cn.v5.mr;

public interface MRMessageListener {
    void onMessage(MRSubscriber subscriber, long messageId, byte[] data);
}
