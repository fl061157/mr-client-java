package cn.v5.mr;

public interface MRSubscriber extends MessageCallback {
    void ackOk(long messageId);
    void ackOk(long messageId, byte[] data);
    void ackFail(long messageId);
    void unSub();
}
